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

public class Constants {
    public static final String NOTIF_ROOT = "/onf"; // Means omid-notification framework TODO Rename when required
    public static final String NOTIF_INTERESTS = NOTIF_ROOT + "/i"; // Means interests TODO Rename when required
    public static final String NOTIF_OBSERVERS = NOTIF_ROOT + "/o"; // Means observers TODO Rename when required
    
    public static final String NOTIF_HBASE_CF_SUFFIX = "-meta"; //
    public static final String HBASE_NOTIFY_SUFFIX = ":notify"; //
    
    public static final int THRIFT_SERVER_PORT = 7911;
}