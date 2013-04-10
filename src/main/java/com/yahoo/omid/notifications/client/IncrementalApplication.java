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

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.yahoo.omid.notifications.thrift.generated.Notification;

public interface IncrementalApplication extends Closeable {
    public String getName();

    public int getPort();

    public Map<String, BlockingQueue<Notification>> getRegisteredObservers();
}
