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


public class ZkTreeUtils {

    public static final String ROOT_NODE = "deltaomid";
    public static final String APPS_NODE = "applications";
    public static final String SERVERS_NODE = "servers";
    
    public static final String ZK_APP_DATA_NODE = "observer-interest-list";
    
    public static String getRootNodePath() {
        return "/" + ROOT_NODE;
    }
    
    public static String getAppsNodePath() {
        return getRootNodePath() + "/" + APPS_NODE;
    }

    public static String getServersNodePath() {
        return getRootNodePath() + "/" + SERVERS_NODE;
    }
}
