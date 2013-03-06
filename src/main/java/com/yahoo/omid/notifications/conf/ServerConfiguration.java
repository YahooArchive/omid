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
package com.yahoo.omid.notifications.conf;

public class ServerConfiguration extends AbstractConfiguration {

protected final static String SCAN_INTERVAL_MS = "scanIntervalMs";
    
    /**
     * Build a default server-side configuration
     */
    public ServerConfiguration() {
        super();
    }
    
    
    /**
     * Get interval between scans in milliseconds
     *
     * @return scan interval in millis
     */
    public long getScanIntervalMs() {
        return getLong(SCAN_INTERVAL_MS, 5000);
    }

    /**
     * Set interval between scans in milliseconds
     *
     * @param scanIntervalMs
     *          scan interval in millis
     */
    public ServerConfiguration setScanIntervalMs(long scanIntervalMs) {
        setProperty(SCAN_INTERVAL_MS, scanIntervalMs);
        return this;
    }
    
}
