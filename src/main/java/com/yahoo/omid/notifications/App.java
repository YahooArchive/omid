package com.yahoo.omid.notifications;

import static com.yahoo.omid.notifications.ZkTreeUtils.ZK_APP_DATA_NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.utils.ZKPaths.PathAndNode;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.metrics.ServerSideAppMetrics;

/**
 * Represents an Application on the server side part of the notification framework. It contains the required meta-data
 * to perform notification to the client side part of the framework
 * 
 */
class App implements PathChildrenCacheListener {

    /**
     * 
     */
    private final AppSandbox appSandbox;

    final Logger logger = LoggerFactory.getLogger(App.class);

    String name;

    ServerSideAppMetrics metrics;

    private PathChildrenCache appsInstanceCache;

    // TODO configurable # of threads
    // A mapping between the hostname:port string representing an app instance and its notifier in the server side
    // Key: The hostname and port as String in the form hostname:port
    // Value: The AppInstanceNotifer in charge of sending notifications to the corresponding app instance
    private ConcurrentHashMap<String, AppInstanceNotifier> notifiers = new ConcurrentHashMap<String, AppInstanceNotifier>();
    // A mapping between an interest and the observer wanting notifications for changes in that interest
    // TODO now we are considering that only one observer is registered per app
    // Otherwise a List of Observers would be required as a second parameter of the list
    // The Thrift class would need also to be modified
    ConcurrentHashMap<String, String> interestObserverMap = new ConcurrentHashMap<String, String>();

    public App(AppSandbox appSandbox, String appName, ZNRecord appData) throws Exception {
        this.appSandbox = appSandbox;
        this.name = appName;

        // Retrieve the obs/interest data from each app data node
        List<String> observersInterests = appData.getListField(ZK_APP_DATA_NODE);
        for (String observerInterest : observersInterests) {
            Iterable<String> tokens = Splitter.on("/").split(observerInterest);
            ArrayList<String> tokenList = Lists.newArrayList(tokens);
            if (tokenList.size() != 2) {
                throw new RuntimeException("Error extracting data from app node: " + appName);
            }
            String obsName = tokenList.get(0);
            String interest = tokenList.get(1);
            logger.trace("Adding interest {} to observer {}", interest, obsName);
            addInterestToObserver(interest, obsName);
        }
        this.metrics = new ServerSideAppMetrics(appName, interestObserverMap.keySet());
        String appPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), this.name);
        appsInstanceCache = new PathChildrenCache(this.appSandbox.zkClient, appPath, false);
        appsInstanceCache.getListenable().addListener(this);
        appsInstanceCache.start();
    }

    public Set<String> getInterests() {
        return interestObserverMap.keySet();
    }

    private void addInterestToObserver(String interest, String observer) {
        String result = interestObserverMap.putIfAbsent(interest, observer);
        if (result != null) {
            logger.warn("Other observer than " + observer + " manifested already interest in " + interest);
        }
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
        case CHILD_ADDED: {
            logger.trace("Instance node added: " + event.getData().getPath());
            addInstance(ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;
        }

        case CHILD_UPDATED: {
            logger.trace("Instance node changed: " + event.getData().getPath());
            break;
        }

        case CHILD_REMOVED: {
            logger.trace("Removing node: " + event.getData().getPath() + " Instances left: " + notifiers.size());
            removeInstance(ZKPaths.getPathAndNode(event.getData().getPath()));
        }
            break;
        case CONNECTION_LOST:
            logger.error("Lost connection with ZooKeeper {}", this.appSandbox.zkClient.getZookeeperClient()
                    .getCurrentConnectionString());
            break;
        case CONNECTION_RECONNECTED:
            logger.warn("Reconnected to ZooKeeper {}", this.appSandbox.zkClient.getZookeeperClient()
                    .getCurrentConnectionString());
            break;
        case CONNECTION_SUSPENDED:
            logger.error("Connection suspended to ZooKeeper {}", this.appSandbox.zkClient.getZookeeperClient()
                    .getCurrentConnectionString());
            break;
        default:
            logger.error("Unknown event type {}", event.getType().toString());
            break;
        }
    }

    public void addInstance(String hostnameAndPort) {
        final HostAndPort hp = HostAndPort.fromString(hostnameAndPort);
        AppInstanceNotifier notifier = new AppInstanceNotifier(this, hp);

        if (notifiers.putIfAbsent(hostnameAndPort, notifier) != null) {
            notifier.cancel();
        } else {
            logger.info("Adding notifier to {} for application {}", hp.toString(), name);
            notifier.start();
        }

    }

    public void removeInstance(PathAndNode pathAndNode) {
        String hostnameAndPort = pathAndNode.getNode();
        AppInstanceNotifier notifier = notifiers.remove(hostnameAndPort);
        if (notifier != null) {
            notifier.cancel();
        }
        if (notifiers.size() == 0) { // Remove app node on ZK to trigger app cleaning TODO sync with the remove before?
            try {
                String appPath = pathAndNode.getPath();
                appSandbox.zkClient.delete().forPath(appPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "App [name=" + name + ", notifiers=" + Arrays.toString(notifiers.keySet().toArray(new String[] {}))
                + ", interestObserverMap=" + interestObserverMap + "]";
    }

    public SynchronousQueue<UpdatedInterestMsg> getHandoffQueue() {
        return appSandbox.getHandoffQueue();
    }

}