package com.yahoo.omid.notifications.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationService;
import com.yahoo.omid.notifications.thrift.generated.NotificationService.Client;

/**
 * Starts a server and waits for the server connection information. Then starts the NotificationClient and 
 * feeds notifications into the queue.
 */
// TODO watch ZooKeeper for new servers and connect to them
class NotificationDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(NotificationDispatcher.class);
    private final NotificationManager notificationManager;
    private Thread clientThread;

    /**
     * @param notificationManager
     */
    NotificationDispatcher(NotificationManager notificationManager) {
        this.notificationManager = notificationManager;
    }

    public void stop() {
        if (clientThread != null) {
            clientThread.interrupt();
        }
    }
    
    private Map<HostAndPort, NotificationClient> clients = new HashMap<HostAndPort, NotificationClient>();

    /**
     * Server connection information, start the NotificationClient
     */
    public synchronized void serverStarted(HostAndPort hostAndPort, String observer) throws TException {

        if (clients.containsKey(hostAndPort)) {
            // stop it first
            serverStoped(hostAndPort, observer);
        }

        BlockingQueue<Notification> observerQueue = this.notificationManager.app.getRegisteredObservers().get(observer);

        String server = hostAndPort.toString();
        // TODO use executor
        NotificationClient notificationClient = new NotificationClient(observerQueue, hostAndPort);
        clientThread = new Thread(notificationClient, "NotificationClient-" + observer + "-" + server);
        clientThread.start();
        clients.put(hostAndPort, notificationClient);
    }

    public synchronized void serverStoped(HostAndPort hostAndPort, String observer) {
        NotificationClient client = clients.remove(hostAndPort);
        if (client != null) {
            client.stop();
        }
    }

    /**
     * Pulls batches of notifications from the server and puts them on the queue. When finished, repeats.
     */
    private static class NotificationClient implements Runnable {
        BlockingQueue<Notification> observerQueue;
        HostAndPort hostAndPort;
        volatile boolean running;

        public NotificationClient(BlockingQueue<Notification> observerQueue, HostAndPort hostAndPort) {
            super();
            this.observerQueue = observerQueue;
            this.hostAndPort = hostAndPort;
            this.running = true;
        }
        
        private Client initializeConnection() throws TException {
            String host = hostAndPort.getHostText();
            int port = hostAndPort.getPort();
            TTransport transport = new TFramedTransport(new TSocket(host, port));
            TProtocol protocol = new TBinaryProtocol(transport);
            Client notificationClient = new NotificationService.Client(protocol);
            try {
                transport.open();
            } catch (TTransportException e) {
                logger.error("Cannot initialize communication with {}:{}", new Object[] { host, port, e });
                throw new TException(e);
            }
            logger.trace("Notifier client started");

            return notificationClient;
        }

        @Override
        public void run() {
            try {
                Client notificationClient = initializeConnection();

                while (running) {
                    for (Notification notification : notificationClient.getNotifications()) {
                        observerQueue.put(notification);
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted, shutting down", e);
            } catch (TException e) {
                logger.error("Unexpected exception, shutting down", e);
            }
        }
        
        public void stop() {
            running = false;
        }

    }
}
