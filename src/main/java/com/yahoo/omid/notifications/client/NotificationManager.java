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
package com.yahoo.omid.notifications.client;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.utils.ZKPaths.PathAndNode;
import com.yahoo.omid.notifications.ZkTreeUtils;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;
import com.yahoo.omid.notifications.metrics.ClientSideAppMetrics;

public class NotificationManager {

    static final Logger logger = LoggerFactory.getLogger(NotificationManager.class);

    final IncrementalApplication app;

    final ClientSideAppMetrics metrics;
    
    final ZNRecordSerializer serializer = new ZNRecordSerializer();

    private NotificationDispatcher dispatcher;

    private CuratorFramework zkClient;

    private final Set<PathChildrenCache> serverCaches;

    public NotificationManager(IncrementalApplication app, ClientSideAppMetrics metrics, CuratorFramework zkClient) {
        this.app = app;
        this.metrics = metrics;
        this.zkClient = zkClient;
        this.serverCaches = new HashSet<PathChildrenCache>();
    }

    public void start() throws Exception {
        dispatcher = new NotificationDispatcher(this);
        ServerChangesListener listener = new ServerChangesListener();
        
        for (String observer : app.getRegisteredObservers().keySet()) {
            String path = ZkTreeUtils.getServersNodePath() + "/" + app.getName() + "/"
                    + observer;
            PathChildrenCache pcc = new PathChildrenCache(this.zkClient, path, false, new ThreadFactoryBuilder()
                    .setNameFormat("ZK App Listener [" + path + "]").build());
            pcc.getListenable().addListener(listener);
            serverCaches.add(pcc);
            pcc.start(true);
            for (ChildData cd : pcc.getCurrentData()) {
                addServer(cd.getPath());
            }
        }
    }

    public void stop() {
        try {
            app.close();
        } catch (IOException e) {
            logger.error("Cannot correctly close application {}", app.getName(), e);
        }
        dispatcher.stop();
    }

    private class ServerChangesListener implements PathChildrenCacheListener {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
                case CHILD_ADDED: {
                    logger.trace("Server Node added : {}", event.getData().getPath());

                    addServer(event.getData().getPath());
                    break;
                }
                case CHILD_UPDATED: {
                    logger.trace("Server Node changed: " + event.getData().getPath());
                    // TODO remove and add
                    break;
                }
                case CHILD_REMOVED:
                    logger.trace("Server Node removed: " + event.getData().getPath());
                    // TODO remove
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

    public void addServer(String path) throws TException {
        PathAndNode serverAndPath = ZKPaths.getPathAndNode(path);
        String server = serverAndPath.getNode();
        String observer = ZKPaths.getNodeFromPath(serverAndPath.getPath());

        HostAndPort hostAndPort = HostAndPort.fromString(server);
        dispatcher.serverStarted(hostAndPort, observer);
    }
}
