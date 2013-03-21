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

import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

public class ClientConfiguration extends AbstractConfiguration {

    private static Logger LOG = LoggerFactory.getLogger(ClientConfiguration.class);

    protected final static String OMID_SERVER = "omidServer";

    /**
     * Build a default client-side configuration
     */
    public ClientConfiguration() {
        super();

        try {
            // if there is an omid.client.properties file in the classpath, we use it for configuration parameters
            URL configURL = Resources.getResource("omid.client.properties");
            try {
                addConfiguration(new PropertiesConfiguration(configURL));
                LOG.info("Read omid configuration from {}", configURL.toString());
            } catch (ConfigurationException e) {
                throw new RuntimeException("Cannot read configuration file omid.client.properties from classpath");
            }
        } catch (IllegalArgumentException e) {
            LOG.info("No omid.client.properties file found in classpath. Using default configuration parameters");
        }

    }

    /**
     * Get Omid TSO server to connect
     * 
     * @return Omid's TSO server
     */
    public String getOmidServer() {
        return getString(OMID_SERVER, "localhost:1234");
    }

    /**
     * Set Omid's TSO server to connect
     * 
     * @param omidServer
     *            Omid server to connect
     */
    public ClientConfiguration setOmidServer(String omidServer) {
        setProperty(OMID_SERVER, omidServer);
        return this;
    }

}
