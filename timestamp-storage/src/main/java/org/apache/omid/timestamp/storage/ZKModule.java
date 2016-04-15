/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.timestamp.storage;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.omid.zk.ZKUtils;
import org.apache.curator.framework.CuratorFramework;

import javax.inject.Singleton;
import java.io.IOException;

//TODO:IK: move to common?
public class ZKModule extends AbstractModule {

    private final String zkCluster;
    private final String namespace;

    public ZKModule(String zkCluster, String namespace) {
        this.zkCluster = zkCluster;
        this.namespace = namespace;
    }

    @Override
    public void configure() {
    }

    @Provides
    @Singleton
    CuratorFramework provideInitializedZookeeperClient() throws IOException, InterruptedException {
        return ZKUtils.initZKClient(zkCluster, namespace, 10);
    }

}