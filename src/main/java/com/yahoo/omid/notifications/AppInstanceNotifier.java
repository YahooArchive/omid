package com.yahoo.omid.notifications;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;
import com.yahoo.omid.notifications.thrift.generated.NotificationService;
import com.yahoo.omid.notifications.thrift.generated.Started;

/**
 * There's one actor per application instance deployed
 * 
 */
public class AppInstanceNotifier extends Thread {

    /**
     * 
     */
    private final App app;
    private HostAndPort hostAndPort;
    private TTransport transport;
    private NotificationReceiverService.Client appInstanceClient;
    private Interest interest;
    public static final int HOLDOFF_TIME_MS = 100;
    private Thread serverThread;

    private static final Logger logger = LoggerFactory.getLogger(AppInstanceNotifier.class);

    public AppInstanceNotifier(App app, Interest interest, HostAndPort hostAndPort) {
        super("[" + app.name + "]/[" + hostAndPort.toString() + "]-notifier");
        this.app = app;
        this.interest = interest;
        this.hostAndPort = hostAndPort;
    }

    @Override
    public void run() {
        if (!preStart()) {
            return;
        }

        // Start NotificationService

        NotificationServer server = new NotificationServer(app.getHandoffQueue(interest));
        serverThread = new Thread(server, "NotificationServer-" + interest);
        serverThread.start();
        try {
            server.waitForServing();
        } catch (InterruptedException e) {
            this.app.logger.error("Server didn't start, aborting", e);
            return;
        }
        HostAndPort hostAndPort;
        try {
            hostAndPort = server.getHostAndPort();
        } catch (UnknownHostException e) {
            this.app.logger.error("Could not get server host and port, aborting", e);
            return;
        }

        String host = hostAndPort.getHostText();
        int port = hostAndPort.getPort();
        String observer = this.app.interestObserverMap.get(interest);

        if (observer == null) {
            this.app.logger.warn(this.app.name + " app notifier could not send notification to instance on "
                    + hostAndPort.toString() + " because target observer has been removed.");
        }

        // Notify interested clients that the server is running on the given host and port
        // TODO handle new clients
        try {
            Started started = new Started(host, port, observer);
            appInstanceClient.serverStarted(started);
        } catch (TException te) {
            this.app.logger.warn(
                    "Communication with app instance [{}]/[{}] is broken. Will try to reconnect after {} ms.",
                    new String[] { app.name, hostAndPort.toString(), String.valueOf(HOLDOFF_TIME_MS) }, te);
        }

        if (transport != null) {
            transport.close();
        }
        this.app.logger.trace("App Notifier stopped");
    }

    public boolean preStart() {
        // Start Thrift communication
        transport = new TFramedTransport(new TSocket(hostAndPort.getHostText(), hostAndPort.getPort()));
        TProtocol protocol = new TBinaryProtocol(transport);
        appInstanceClient = new NotificationReceiverService.Client(protocol);
        try {
            transport.open();
        } catch (TTransportException e) {
            logger.error("Cannot initialize communication with {}", hostAndPort.toString(), e);
            return false;
        }
        this.app.logger.trace("App Notifier started");
        return true;
    }

    public void cancel() {
        interrupt();
    }

    /**
     * Implements the NotificationService, clients pull notifications from here
     */
    private static class NotificationServer implements Runnable, NotificationService.Iface {

        private static final Logger logger = LoggerFactory.getLogger(NotificationServer.class);
        ServerSocket socket;

        private TServer server;
        private CountDownLatch latch = new CountDownLatch(1);
        private BlockingQueue<Notification> handoffQueue;

        public NotificationServer(BlockingQueue<Notification> handoffQueue) {
            this.handoffQueue = handoffQueue;
        }

        @Override
        public void run() {
            try {
                TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(0);
                NotificationService.Processor<NotificationServer> processor = new NotificationService.Processor<NotificationServer>(
                        this);
                server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));

                // TODO dont use reflection here
                Field serverSocket = TNonblockingServerSocket.class.getDeclaredField("serverSocket_");
                serverSocket.setAccessible(true);
                socket = (ServerSocket) serverSocket.get(serverTransport);
                latch.countDown();
                server.serve();
            } catch (TTransportException e) {
                logger.error("Thrift server error. Stopping.", e);
            } catch (Exception e) {
                logger.error("Couldnt get socket", e);
            } finally {
                stop();
            }
        }

        private void stop() {
            if (server != null) {
                server.stop();
            }
        }

        @Override
        /** 
         * For each pull request, send what's avaialable on the queue for batching efficiency
         */
        public List<Notification> getNotifications() throws TException {
            List<Notification> notifications = new ArrayList<Notification>();
            // TODO use drainTo(notification, maxBatchSize)
            handoffQueue.drainTo(notifications);
            return notifications;
        }

        public void waitForServing() throws InterruptedException {
            latch.await();
        }

        public HostAndPort getHostAndPort() throws UnknownHostException {
            InetSocketAddress isa = (InetSocketAddress) socket.getLocalSocketAddress();
            return HostAndPort.fromParts(InetAddress.getLocalHost().getHostAddress(), isa.getPort());
        }
    }
}
