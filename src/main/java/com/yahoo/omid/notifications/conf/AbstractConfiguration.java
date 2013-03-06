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

import java.util.List;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang.StringUtils;

/**
* Includes common configuration attributes
*/
public abstract class AbstractConfiguration extends CompositeConfiguration {

    // Zookeeper
    protected final static String ZK_SERVERS = "zkServers";
    
    protected AbstractConfiguration() {
        super();
        // add configuration for system properties
        addConfiguration(new SystemConfiguration());
    }
    
    /**
     * Set Zk Ensemble
     *
     * @param zkServers zk ensemble with this format: host1:port1,host2:port2...
     */
    public void setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
    }

    /**
     * Get Zk Ensemble
     * 
     * @return zk ensemble
     */
    public String getZkServers() {
        List<Object> servers = getList(ZK_SERVERS, null);
        if (null == servers || 0 == servers.size()) {
            return "localhost";
        }
        return StringUtils.join(servers, ",");
    }
}
