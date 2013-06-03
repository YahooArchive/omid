package com.yahoo.omid.notifications;

import static com.yahoo.omid.notifications.ZkTreeUtils.ZK_APP_DATA_NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.utils.ZKPaths.PathAndNode;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.metrics.ServerSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;

/**
 * Represents an Application on the server side part of the notification framework. It contains the required meta-data
 * to perform notification to the client side part of the framework
 * 
 */
class App {

    /**
     * 
     */
    private final AppSandbox appSandbox;

    final Logger logger = LoggerFactory.getLogger(App.class);

    String name;

    ServerSideAppMetrics metrics;

    // TODO configurable nb threads
    private ConcurrentHashMap<HostAndPort, Map<Interest, AppInstanceNotifier>> notifiers = new ConcurrentHashMap<HostAndPort, Map<Interest, AppInstanceNotifier>>();

    // A mapping between an interest and the observer wanting notifications for changes in that interest
    // TODO now we are considering that only one observer is registered per app
    // Otherwise a List of Observers would be required as a second parameter of the list
    // The Thrift class would need also to be modified
    ConcurrentHashMap<Interest, String> interestObserverMap = new ConcurrentHashMap<Interest, String>();

    public App(AppSandbox appSandbox, String appName, ZNRecord appData, Coordinator coordinator) throws Exception {
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
            String interestName = tokenList.get(1);
            Interest interest = Interest.fromString(interestName);
            logger.info("Adding interest {} to observer {}", interest, obsName);
            addInterestToObserver(interest, obsName);
        }
        this.metrics = new ServerSideAppMetrics(appName, interestObserverMap.keySet());
        String appPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), this.name);
        for (Interest interest : interestObserverMap.keySet()) {
            AppInstanceNotifier notifier = new AppInstanceNotifier(this, interest, coordinator);
            notifier.start();
        }
    }

    public Set<Interest> getInterests() {
        return interestObserverMap.keySet();
    }

    private void addInterestToObserver(Interest interest, String observer) {
        String result = interestObserverMap.putIfAbsent(interest, observer);
        if (result != null) {
            logger.warn("Other observer than " + observer + " manifested already interest in " + interest);
        }
    }

    @Override
    public String toString() {
        return "App [name=" + name + ", notifiers=" + Arrays.toString(notifiers.keySet().toArray(new String[] {}))
                + ", interestObserverMap=" + interestObserverMap + "]";
    }

    public BlockingQueue<Notification> getHandoffQueue(Interest interest) {
        return appSandbox.getHandoffQueue(interest);
    }

}