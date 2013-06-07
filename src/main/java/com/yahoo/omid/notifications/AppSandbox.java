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

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.thrift.generated.Notification;

public class AppSandbox {

    private static final Logger logger = LoggerFactory.getLogger(AppSandbox.class);

    private ScannerSandbox scannerSandbox;

    private Map<String, App> registeredApps = new Hashtable<String, App>();

    private Coordinator coordinator;

    public AppSandbox(CuratorFramework zkClient, ScannerSandbox scannerSandbox, Coordinator coordinator) throws Exception {
        this.scannerSandbox = scannerSandbox;
        this.coordinator = coordinator;
    }

    public synchronized void addApplicationInstance(String appName, ZNRecord appData) throws Exception {
        logger.info("app name: " + appName);
        logger.info("app data: " + appData);
        if (!appName.equals(appData.getId())) {
            logger.error("App data doesn't correspond to app");
            throw new RuntimeException("App data retrieved doesn't corresponds to app: " + appName);
        }
        App app = registeredApps.get(appName);
        if (app != null) {
            logger.info("App already configured, incrementing live instances : {}", appName);
            app.addLiveInstance();
            return;
        }
        app = new App(this, appName, appData, coordinator);
        scannerSandbox.registerInterestsFromApplication(app);
        registeredApps.put(appName, app);
        app.start();
        logger.info("Registered new application {}", appData);
        // NOTE: It is not necessary to create the instances. It is triggered automatically by curator
        // through the App.childEvent() callback when constructing the App object (particularly, when
        // registering the interest in the Zk app node)
    }

    public synchronized App removeApplicationInstance(String appName) throws Exception {
        App app = registeredApps.remove(appName);
        if (app == null) {
            throw new NullPointerException("App " + appName + " was not registered in AppSanbox");
        }
        app.removeLiveInstance();
        if (app.getLiveInstances() == 0) {
            registeredApps.remove(appName);
            scannerSandbox.removeInterestsFromApplication(app);
            logger.info("Removed application {}", appName);
        }
        return app;
    }

    public BlockingQueue<Notification> getHandoffQueue(Interest interest) {
        return scannerSandbox.getHandoffQueue(interest);
    }

}
