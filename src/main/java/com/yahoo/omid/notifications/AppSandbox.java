/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.yahoo.omid.notifications;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;

public class AppSandbox implements PathChildrenCacheListener {

    private static final Logger logger = LoggerFactory.getLogger(AppSandbox.class);

    CuratorFramework zkClient;

    private ScannerSandbox scannerSandbox;

    private PathChildrenCache appsCache;

    private ConcurrentHashMap<String, App> registeredApps = new ConcurrentHashMap<String, App>();

    public AppSandbox(CuratorFramework zkClient, ScannerSandbox scannerSandbox) throws Exception {
        this.zkClient = zkClient;
        this.scannerSandbox = scannerSandbox;
        appsCache = new PathChildrenCache(this.zkClient, ZkTreeUtils.getAppsNodePath(), false);
        appsCache.getListenable().addListener(this);
    }

    public void startWatchingAppsNode() throws Exception {
        appsCache.start(true);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

        switch (event.getType()) {
        case CHILD_ADDED: {
            logger.trace("App Node added : {}", event.getData().getPath());
            createApplication(ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;
        }
        case CHILD_UPDATED: {
            logger.trace("App Node changed: " + event.getData().getPath());
            break;
        }
        case CHILD_REMOVED: {
            logger.trace("App Node removed: " + event.getData().getPath());
            removeApplication(ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;
        }
        case CONNECTION_LOST:
            logger.error("Lost connection with ZooKeeper {}", zkClient.getZookeeperClient()
                    .getCurrentConnectionString());
            break;
        case CONNECTION_RECONNECTED:
            logger.warn("Reconnected to ZooKeeper {}", zkClient.getZookeeperClient().getCurrentConnectionString());
            break;
        case CONNECTION_SUSPENDED:
            logger.error("Connection suspended to ZooKeeper {}", zkClient.getZookeeperClient()
                    .getCurrentConnectionString());
            break;
        default:
            logger.error("Unknown event type {}", event.getType().toString());
            break;
        }
    }

    public void createApplication(String appName) throws Exception {
        String appNodePath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), appName);
        byte[] rawData = zkClient.getData().forPath(appNodePath);
        ZNRecord appData = (ZNRecord) new ZNRecordSerializer().deserialize(rawData);
        if (!appName.equals(appData.getId())) {
            throw new RuntimeException("App data retrieved doesn't corresponds to app: " + appName);
        }
        App app = new App(this, appName, appData);
        if (null == registeredApps.putIfAbsent(appName, app)) {
            scannerSandbox.registerInterestsFromApplication(app);
            logger.info("Registered new application {}", appData);
        }
        // NOTE: It is not necessary to create the instances. It is triggered automatically by curator
        // through the App.childEvent() callback when constructing the App object (particularly, when
        // registering the interest in the Zk app node)
    }

    private App removeApplication(String appName) throws Exception {
        App removedApp = registeredApps.remove(appName);
        if (removedApp != null) {
            scannerSandbox.removeInterestsFromApplication(removedApp);
            logger.info("Removed application {}", appName);
        } else {
            throw new Exception("App " + appName + " was not registered in AppSanbox");
        }
        return removedApp;
    }

    public SynchronousQueue<UpdatedInterestMsg> getHandoffQueue() {
        return scannerSandbox.getHandoffQueue();
    }

    public CuratorFramework getZKClient() {
        return zkClient;
    }

}
