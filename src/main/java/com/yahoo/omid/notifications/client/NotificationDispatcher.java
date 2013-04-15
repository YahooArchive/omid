package com.yahoo.omid.notifications.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;
import com.yahoo.omid.notifications.thrift.generated.ObserverOverloaded;

class NotificationDispatcher implements Runnable, NotificationReceiverService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(NotificationDispatcher.class);
    private final NotificationManager notificationManager;
    public static final int ENQUEUE_TIMEOUT_MS = 500;

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
    }

    /*
     * Thrift generated
     */
    @Override
    public void notify(Notification notification) throws ObserverOverloaded, TException {
        this.notificationManager.metrics.notificationReceivedEvent();
        BlockingQueue<Notification> observerQueue = this.notificationManager.app.getRegisteredObservers().get(
                notification.getObserver());
        if (observerQueue != null) {
            boolean offered = false;
            try {
                offered = observerQueue.offer(notification, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!offered) {
                throw new ObserverOverloaded(notification.getObserver());
            }
        } else {
            logger.warn(
                    "No target observer for notification {}:{}:{}:{}",
                    new String[] { Bytes.toString(notification.getTable()),
                            Bytes.toString(notification.getColumnFamily()), Bytes.toString(notification.getColumn()),
                            Bytes.toString(notification.getRowKey()) });
        }

    }
}