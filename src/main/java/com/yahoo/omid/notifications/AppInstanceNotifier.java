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
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.notifications.metrics.ServerSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationService;

/**
 * There's one actor per application instance deployed
 * 
 */
public class AppInstanceNotifier {

    /**
     * 
     */
    private final App app;
    private Interest interest;
    public static final int HOLDOFF_TIME_MS = 100;
    private Thread serverThread;
    private Coordinator coordinator;
    private NotificationServer server;
    private HostAndPort hostAndPort;
    private String observer;

    private static final Logger logger = LoggerFactory.getLogger(AppInstanceNotifier.class);

    public AppInstanceNotifier(App app, Interest interest, Coordinator coordinator) {
        this.app = app;
        this.interest = interest;
        this.coordinator = coordinator;
    }

    public void start() {

        // Start NotificationService
        observer = this.app.interestObserverMap.get(interest);
        if (observer == null) {
            this.app.logger.warn(this.app.name + " app notifier could not send notification to instance on "
                    + hostAndPort.toString() + " because target observer has been removed.");
            return;
        }
        
        server = new NotificationServer(app.getHandoffQueue(interest), app.metrics, observer);

        // TODO use executor
        serverThread = new Thread(server, "NotificationServer-" + interest);
        serverThread.start();
        try {
            server.waitForServing();
        } catch (InterruptedException e) {
            this.app.logger.error("Server didn't start, aborting", e);
            return;
        }
        try {
            hostAndPort = server.getHostAndPort();
        } catch (UnknownHostException e) {
            this.app.logger.error("Could not get server host and port, aborting", e);
            return;
        }

        coordinator.registerInstanceNotifier(hostAndPort, app.name, observer);

        this.app.logger.trace("App Notifier stopped");
    }

    public void stop() {
        server.stop();
        coordinator.removeInstanceNotifier(hostAndPort, app.name, observer);
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
        private ServerSideAppMetrics metrics;
        private String observer;

        public NotificationServer(BlockingQueue<Notification> handoffQueue, ServerSideAppMetrics metrics, String observer) {
            this.handoffQueue = handoffQueue;
            this.metrics = metrics;
            this.observer = observer;
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
            int count = handoffQueue.drainTo(notifications, 100);
            if (count == 0) {
                // if we didn't get any, wait for one at least
                try {
                    notifications.add(handoffQueue.take());
                    count = 1;
                } catch (InterruptedException e) {
                    throw new TException(e);
                }
            }
            metrics.notificationSentEvent(observer, count);
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
