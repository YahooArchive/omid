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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.NotificationException;
import com.yahoo.omid.notifications.metrics.ClientSideAppMetrics;

public class NotificationManager {

    static final Logger logger = LoggerFactory.getLogger(NotificationManager.class);

    private static final long TIMEOUT = 3;

    final IncrementalApplication app;

    final ClientSideAppMetrics metrics;

    private final ExecutorService notificatorAcceptorExecutor;
    private NotificationDispatcher dispatcher;

    public NotificationManager(IncrementalApplication app, ClientSideAppMetrics metrics) {
        this.app = app;
        this.metrics = metrics;
        this.notificatorAcceptorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                app.getName() + "-Notificator-%d").build());
    }

    public void start() throws NotificationException {
        dispatcher = new NotificationDispatcher(this);
        notificatorAcceptorExecutor.execute(dispatcher);
    }

    public void stop() {
        try {
            app.close();
        } catch (IOException e) {
            logger.error("Cannot correctly close application {}", app.getName(), e);
        }
        dispatcher.stop();
        notificatorAcceptorExecutor.shutdownNow();

    }

}
