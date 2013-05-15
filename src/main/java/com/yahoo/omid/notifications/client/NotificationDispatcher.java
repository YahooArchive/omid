package com.yahoo.omid.notifications.client;

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

    /**
     * Server connection information, start the NotificationClient
     */
    // TODO call this when a new server starts
    public void serverStarted(HostAndPort hostAndPort, String observer) throws TException {

        String host = hostAndPort.getHostText();
        int port = hostAndPort.getPort();
        BlockingQueue<Notification> observerQueue = this.notificationManager.app.getRegisteredObservers().get(observer);
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        TProtocol protocol = new TBinaryProtocol(transport);
        Client appInstanceClient = new NotificationService.Client(protocol);
        try {
            transport.open();
        } catch (TTransportException e) {
            logger.error("Cannot initialize communication with {}:{}", new Object[] { host, port, e });
            throw new TException(e);
        }
        logger.trace("Notifier client started");

        String server = host + ":" + port;
        // TODO use executor
        clientThread = new Thread(new NotificationClient(observerQueue, appInstanceClient), "NotificationClient-"
                + observer + "-" + server);
        clientThread.start();
    }

    /**
     * Pulls batches of notifications from the server and puts them on the queue. When finished, repeats.
     */
    private static class NotificationClient implements Runnable {
        BlockingQueue<Notification> observerQueue;
        Client notificationClient;

        public NotificationClient(BlockingQueue<Notification> observerQueue, Client notificationClient) {
            super();
            this.observerQueue = observerQueue;
            this.notificationClient = notificationClient;
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
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

    }
}
