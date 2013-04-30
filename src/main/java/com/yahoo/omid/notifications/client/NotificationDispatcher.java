package com.yahoo.omid.notifications.client;

import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;
import com.yahoo.omid.notifications.thrift.generated.NotificationService;
import com.yahoo.omid.notifications.thrift.generated.NotificationService.Client;
import com.yahoo.omid.notifications.thrift.generated.Started;

/**
 * Starts a server and waits for the server connection information. Then starts the NotificationClient and 
 * feeds notifications into the queue.
 */
class NotificationDispatcher implements Runnable, NotificationReceiverService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(NotificationDispatcher.class);
    private final NotificationManager notificationManager;
    private Thread clientThread;

    /**
     * @param notificationManager
     */
    NotificationDispatcher(NotificationManager notificationManager) {
        this.notificationManager = notificationManager;
    }

    private TServer server;

    @Override
    public void run() {
        try {
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(
                    this.notificationManager.app.getPort());
            NotificationReceiverService.Processor<NotificationDispatcher> processor = new NotificationReceiverService.Processor<NotificationDispatcher>(
                    this);
            server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
            NotificationManager.logger.info("App " + this.notificationManager.app.getName()
                    + " listening for notifications on port " + this.notificationManager.app.getPort());
            server.serve();
        } catch (TTransportException e) {
            logger.error("Thrift server error. Stopping.", e);
        } finally {
            stop();
        }
    }

    public void stop() {
        if (server != null) {
            server.stop();
            NotificationManager.logger.info("App " + this.notificationManager.app.getName()
                    + " stopped listening for notifications on port " + this.notificationManager.app.getPort());
        }
        if (clientThread != null) {
            clientThread.interrupt();
        }
    }

    @Override
    /**
     * Server connection information, start the NotificationClient
     */
    public void serverStarted(Started started) throws TException {
        String host = started.getHost();
        int port = started.getPort();
        String observer = started.getObserver();

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

        clientThread = new Thread(new NotificationClient(observerQueue, appInstanceClient), "NotificationClient-"
                + observer);
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
