package com.yahoo.omid.notifications;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

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
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;
import com.yahoo.omid.notifications.thrift.generated.ObserverOverloaded;
import com.yammer.metrics.core.TimerContext;

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
    public static final int HOLDOFF_TIME_MS = 100;

    private static final Logger logger = LoggerFactory.getLogger(AppInstanceNotifier.class);

    public AppInstanceNotifier(App app, HostAndPort hostAndPort) {
        super("[" + app.name + "]/[" + hostAndPort.toString() + "]-notifier");
        this.app = app;
        this.hostAndPort = hostAndPort;
    }

    @Override
    public void run() {
        if (!preStart()) {
            return;
        }
        while (!Thread.interrupted()) {
            UpdatedInterestMsg msg = null;
            try {
                msg = app.getHandoffQueue().poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (msg == null) {
                logger.warn("Could not get any notification after 10s");
                continue;
            }
            String updatedInterest = msg.interest;
            byte[] updatedRowKey = msg.rowKey;

            String observer = this.app.interestObserverMap.get(updatedInterest);

            if (observer != null) {
                Interest interest = Interest.fromString(updatedInterest);
                Notification notification = new Notification(observer, ByteBuffer.wrap(interest
                        .getTableAsHBaseByteArray()), ByteBuffer.wrap(updatedRowKey), // This is the row that
                                                                                      // has been modified
                        ByteBuffer.wrap(interest.getColumnFamilyAsHBaseByteArray()), ByteBuffer.wrap(interest
                                .getColumnAsHBaseByteArray()));
                TimerContext timer = app.metrics.startNotificationSendTimer(updatedInterest);
                try {
                    appInstanceClient.notify(notification);
                    this.app.metrics.notificationSentEvent();
                    timer.stop();
                    // logger.trace("App notifier sent notification " + notification + " to app running on " +
                    // host + ":" + port);
                } catch (ObserverOverloaded oo) {
                    timer.stop();
                    app.logger.warn(
                            "App instance [{}]/[{}] saturated for observer [{}]. Pausing for {} ms",
                            new String[] { app.name, hostAndPort.toString(),
                                    (oo.getObserver() != null ? oo.getObserver() : "Unknown observer"),
                                    String.valueOf(HOLDOFF_TIME_MS) });
                    try {
                        Thread.sleep(HOLDOFF_TIME_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } catch (TException te) {
                    timer.stop();
                    this.app.logger.warn(
                            "Communication with app instance [{}]/[{}] is broken. Will try to reconnect after {} ms.",
                            new String[] { app.name, hostAndPort.toString(), String.valueOf(HOLDOFF_TIME_MS) }, te);
                    try {
                        transport.close();
                        try {
                            Thread.sleep(HOLDOFF_TIME_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        transport.open();
                    } catch (TTransportException e) {
                        this.app.logger.error(
                                "Could not (re)establish communication with app [{}]/[{}]. Stopping app notifier",
                                new String[] { app.name, hostAndPort.toString() }, e);
                        break;
                    }
                }
            } else {
                this.app.logger.warn(this.app.name + " app notifier could not send notification to instance on "
                        + hostAndPort.toString() + " because target observer has been removed.");
            }
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

}