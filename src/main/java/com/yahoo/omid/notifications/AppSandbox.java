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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

public class AppSandbox {

    private static final Logger logger = Logger.getLogger(AppSandbox.class);

    private ScannerSandbox scannerSandbox;
    private ActorSystem appSandboxActorSystem;
    private ActorRef appInstanceRedirector;

    private ConcurrentHashMap<String, App> registeredApps = new ConcurrentHashMap<String, App>();
    // A mapping between an interest and the applications interested in.
    // Key: The interest as String
    // Value: The list of interested apps
    private ConcurrentHashMap<String, List<String>> interestAppsMap = new ConcurrentHashMap<String, List<String>>();

    
    // TODO We need a reference to scanner sandbox
    public AppSandbox() {
        scannerSandbox = new ScannerSandbox(this);
        appSandboxActorSystem = ActorSystem.create("AppSandbox");
        appInstanceRedirector = appSandboxActorSystem.actorOf(
                new Props(new UntypedActorFactory() {
                    public UntypedActor create() {
                        return new AppInstanceRedirector();
                    }
                }), "AppInstanceRedirector");
    }

    public void registerApplicationInstance(String appName, String hostnameAndPort) {
        App app = registeredApps.get(appName);
        if (app == null) {
            app = new App(appSandboxActorSystem, appName);
            App previousApp = registeredApps.putIfAbsent(appName, app);
            if (previousApp != null) {
                app = previousApp;
            }
        }
        app.addHostRunningApp(hostnameAndPort);        
    }

    public void deregisterApplicationInstance(String appName, String hostnameAndPort) {
        App app = registeredApps.get(appName);
        if (app == null || !app.isInstanceRunningInHost(hostnameAndPort)) {
            logger.warn("Application " + appName + " running in host " + hostnameAndPort + " is not registered");
            return;
        }
        app.removeHostRunningApp(hostnameAndPort);
        synchronized(this) {
            if (app.hostRunningAppCount() == 0) {
                registeredApps.remove(appName);
            }
        }
        logger.trace("Application " + appName + " removed from host " + hostnameAndPort);
    }
    
    public void addObserverInterest(String appName, String interest, String observer) {
        App app = registeredApps.get(appName);
        if (app == null) {
            logger.warn("Observer " + observer + " not added to interest " + interest + " because application "
                    + appName + " does not exists");
            return;
        }
        app.addInterestToObserver(interest, observer);
        
        List<String> appList = new ArrayList<String>();
        appList.add(appName);
        appList = interestAppsMap.putIfAbsent(interest, appList);
        if(appList != null) {
            appList.add(appName);
            interestAppsMap.put(interest, appList);
        }
        try {
            scannerSandbox.addScannerContainer(interest);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void removeObserverInterest(String appName, String interest, String observer) {
        App app = registeredApps.get(appName);
        if (app == null) {
            logger.warn("Observer " + observer + " not removed from interest " + interest + " because application "
                    + appName + " does not exists anymore");
            return;
        }
        app.removeInterestFromObserver(interest, observer);
        List<String> appList = interestAppsMap.get(interest);
        appList.remove(appName);
        if(appList.isEmpty()) {
            interestAppsMap.remove(interest);
        }
        try {
            scannerSandbox.removeScannerContainer(interest);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Added to be used when the ZK node representing an interest is removed
     */
    public void removeInterest(String appName, String interest) {
        App app = registeredApps.get(appName);
        if (app == null) {
            logger.warn("Interest " + interest + " not removed because application "
                    + appName + " does not exists anymore");
            return;
        }
        app.removeInterest(interest);
        try {
            scannerSandbox.removeScannerContainer(interest);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ActorRef getAppInstanceRedirector() {
        return appInstanceRedirector;
    }
    
    /**
     * There's only one actor to redirect messages to application
     * 
     */
    private class AppInstanceRedirector extends UntypedActor {

        @Override
        public void preStart() {
            logger.trace("App Instance Redirector started");
        }

        @Override
        public void onReceive(Object msg) throws Exception {
            if (msg instanceof UpdatedInterestMsg) {
                String interest = ((UpdatedInterestMsg) msg).interest;
                List<String> appsNames = interestAppsMap.get(interest);
                for(String appName : appsNames) {
                    App app = registeredApps.get(appName);
                    List<ActorRef> instances = app.getHostsRunningInstances();
                    for(ActorRef instance : instances) {
                        instance.tell(msg);
                    }
                }
            } else {
                unhandled(msg);
            }
        }

        @Override
        public void postStop() {
            logger.trace("App Instance Redirector stopped");
        }
    }

}
