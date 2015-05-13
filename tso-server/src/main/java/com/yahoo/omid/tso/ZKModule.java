/**
 * Copyright 2011-2015 Yahoo Inc.
 *
 * Some portions of this file are Copyright 2001-2015 Apache Software
 * Foundation (ASF). Taken from Apache Commons JCS project
 * (https://commons.apache.org/proper/commons-jcs/) Licensed under
 * Apache License, Version 2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ZKModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(ZKModule.class);

    private final TSOServerCommandLineConfig config;

    public ZKModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    public void configure() {
    }

    @Provides
    @Singleton
    CuratorFramework provideZookeeperClient() throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", config.getZKCluster());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                                      .namespace(OMID_NAMESPACE)
                                      .connectString(config.getZKCluster())
                                      .retryPolicy(retryPolicy)
                                      .build();

        return zkClient;
    }

    @Provides
    @Singleton
    @Named(TSO_HOST_AND_PORT_KEY)
    String provideTSOHostAndPort() throws SocketException,
            UnknownHostException {
        // Build TSO host:port string and validate it
        final String tsoNetIfaceName = config.getNetworkIface();
        InetAddress addr = getIPAddressFromNetworkInterface(tsoNetIfaceName);
        final int tsoPort = config.getPort();

        String tsoHostAndPortAsString = addr.getHostAddress() + ":" + tsoPort;
        try {
            HostAndPort.fromString(tsoHostAndPortAsString);
        } catch (IllegalArgumentException e) {
            LOG.error("Cannot parse TSO host:port string {}", tsoHostAndPortAsString);
            throw e;
        }
        return tsoHostAndPortAsString;
    }

    /**
     * Returns an <code>InetAddress</code> object encapsulating what is most
     * likely the machine's LAN IP address.
     * <p/>
     * This method is intended for use as a replacement of JDK method
     * <code>InetAddress.getLocalHost</code>, because that method is ambiguous
     * on Linux systems. Linux systems enumerate the loopback network
     * interface the same way as regular LAN network interfaces, but the JDK
     * <code>InetAddress.getLocalHost</code> method does not specify the
     * algorithm used to select the address returned under such circumstances,
     * and will often return the loopback address, which is not valid for
     * network communication. Details
     * <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037">here</a>.
     * <p/>
     * This method will scan all IP addresses on a particular network interface
     * specified as parameter on the host machine to determine the IP address
     * most likely to be the machine's LAN address. If the machine has multiple
     * IP addresses, this method will prefer a site-local IP address (e.g.
     * 192.168.x.x or 10.10.x.x, usually IPv4) if the machine has one (and will
     * return the first site-local address if the machine has more than one),
     * but if the machine does not hold a site-local address, this method will
     * return simply the first non-loopback address found (IPv4 or IPv6).
     * <p/>
     * If this method cannot find a non-loopback address using this selection
     * algorithm, it will fall back to calling and returning the result of JDK
     * method <code>InetAddress.getLocalHost()</code>.
     * <p/>
     *
     * @param ifaceName
     *             The name of the network interface to extract the IP address
     *             from
     * @throws UnknownHostException
     *             If the LAN address of the machine cannot be found.
     */
    public static InetAddress getIPAddressFromNetworkInterface(String ifaceName)
    throws SocketException, UnknownHostException {

        NetworkInterface iface = NetworkInterface.getByName(ifaceName);
        if (iface == null) {
            throw new IllegalArgumentException(
                    "Network interface " + ifaceName + " not found");
        }

        InetAddress candidateAddress = null;
        Enumeration<InetAddress> inetAddrs = iface.getInetAddresses();
        while (inetAddrs.hasMoreElements()) {
            InetAddress inetAddr = inetAddrs.nextElement();
            if (!inetAddr.isLoopbackAddress()) {
                if (inetAddr.isSiteLocalAddress()) {
                    return inetAddr; // Return non-loopback site-local address
                } else if (candidateAddress == null) {
                    // Found non-loopback address, but not necessarily site-local
                    candidateAddress = inetAddr;
                }
            }
        }

        if (candidateAddress != null) {
            // Site-local address not found, but found other non-loopback addr
            // Server might have a non-site-local address assigned to its NIC
            // (or might be running IPv6 which deprecates "site-local" concept)
            return candidateAddress;
        }

        // At this point, we did not find a non-loopback address.
        // Fall back to returning whatever InetAddress.getLocalHost() returns
        InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
        if (jdkSuppliedAddress == null) {
            throw new UnknownHostException(
                    "InetAddress.getLocalHost() unexpectedly returned null.");
        }
        return jdkSuppliedAddress;
    }

}