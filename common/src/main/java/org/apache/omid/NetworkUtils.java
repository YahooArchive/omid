/*
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
package org.apache.omid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class NetworkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    private static final String LINUX_TSO_NET_IFACE_PREFIX = "eth";
    private static final String MAC_TSO_NET_IFACE_PREFIX = "en";

    public static String getDefaultNetworkInterface() {

        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                String name = networkInterfaces.nextElement().getDisplayName();
                LOG.info("Iterating over network interfaces, found '{}'", name);
                if (name.startsWith(MAC_TSO_NET_IFACE_PREFIX) || name.startsWith(LINUX_TSO_NET_IFACE_PREFIX)) {
                    return name;
                }
            }
        } catch (SocketException ignored) {
            throw new RuntimeException("Failed to find any network interfaces", ignored);
        }
        throw new IllegalArgumentException(String.format("No network '%s*'/'%s*' interfaces found",
                                                         MAC_TSO_NET_IFACE_PREFIX, LINUX_TSO_NET_IFACE_PREFIX));

    }

}
