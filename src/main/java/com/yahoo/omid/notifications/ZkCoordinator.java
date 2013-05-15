package com.yahoo.omid.notifications;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;

public class ZkCoordinator implements Coordinator {
    private static final Logger logger = LoggerFactory.getLogger(ZkCoordinator.class);

    private AppSandbox appSandbox;
    private ZNRecordSerializer serializer = new ZNRecordSerializer();

    private PathChildrenCache appsCache;

    private CuratorFramework zkClient;

    ZkCoordinator(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void registerInstanceNotifier(HostAndPort hostAndport, String app, String observer) {
        StringBuilder path = new StringBuilder();
        path.append(ZkTreeUtils.getServersNodePath())  // root
                .append('/').append(app)               // interest
                .append('/').append(observer)          // observer 
                .append('/').append(hostAndport);      // server 
        try {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path.toString());
        } catch (Exception e) {
            logger.error("Couldn't register instance notifier for app {} observer {} listening at {}",
                    new Object[] {app, observer, hostAndport, e});
        }
    }

    @Override
    public void registerAppSandbox(AppSandbox appSandbox) throws Exception {
        this.appSandbox = appSandbox;
        this.appsCache = new PathChildrenCache(this.zkClient, ZkTreeUtils.getAppsNodePath(), true,
                new ThreadFactoryBuilder().setNameFormat("ZK App Listener [" + ZkTreeUtils.getAppsNodePath() + "]")
                        .build());
        appsCache.getListenable().addListener(new AppChangesListener());
        appsCache.start();

        createOrRecoverServerConfigFromZkTree();
    }

    private void createOrRecoverServerConfigFromZkTree() throws Exception {
        logger.info("Configuring server based on current ZK structure");
        Stat s = zkClient.checkExists().forPath(ZkTreeUtils.getRootNodePath());
        if (s == null) {
            createZkTree();
        } else {
            recoverCurrentZkAppBranch();
        }
        logger.info("Server configuration finished");
    }

    private void createZkTree() {
        logger.info("Creating a new ZK Tree");
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(ZkTreeUtils.getAppsNodePath());
            zkClient.create().creatingParentsIfNeeded().forPath(ZkTreeUtils.getServersNodePath());
        } catch (Exception e) {
            logger.warn("Got unexpected exception, concurrent creation of the tree?");
        }
    }

    private void recoverCurrentZkAppBranch() throws Exception {
        logger.info("Recovering existing ZK Tree");
        String appsNodePath = ZkTreeUtils.getAppsNodePath();
        List<String> appNames = zkClient.getChildren().forPath(appsNodePath);
        for (String appName : appNames) {
            byte[] rawData = zkClient.getData().forPath(appsNodePath + "/" + appName);
            ZNRecord appData = (ZNRecord) serializer.deserialize(rawData);
            appSandbox.createApplication(appName, appData);
        }
    }

    private class AppChangesListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    logger.info("App Node added : {}", event.getData().getPath());
                    logger.info("Deserializing : {}", event.getData().getData());
                    if (event.getData().getData() == null)
                        break;
                    ZNRecord appData = (ZNRecord) serializer.deserialize(event.getData().getData());
                    logger.info("App data : {}", appData);
                    appSandbox.createApplication(ZKPaths.getNodeFromPath(event.getData().getPath()), appData);
                    break;
                }
                case CHILD_UPDATED: {
                    logger.info("App Node changed: " + event.getData().getPath());
                    appSandbox.removeApplication(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    ZNRecord appData = (ZNRecord) serializer.deserialize(event.getData().getData());
                    appSandbox.createApplication(ZKPaths.getNodeFromPath(event.getData().getPath()), appData);
                    break;
                }
                case CHILD_REMOVED:
                    logger.info("App Node removed: " + event.getData().getPath());
                    appSandbox.removeApplication(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    break;
                case CONNECTION_LOST:
                    logger.error("Lost connection with ZooKeeper ");
                    break;
                case CONNECTION_RECONNECTED:
                    logger.warn("Reconnected to ZooKeeper");
                    break;
                case CONNECTION_SUSPENDED:
                    logger.error("Connection suspended to ZooKeeper");
                    break;
                default:
                    logger.error("Unknown event type {}", event.getType().toString());
                    break;
            }
        }
    }
}
