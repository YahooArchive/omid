package com.yahoo.omid;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;

/**
 * This class contains functionality that is useful for the Omid tests.
 */
public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private static final int DEFAULT_ZK_PORT = 2181;

    public static int getFreeLocalPort() throws IOException {

        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }

    }

    public static TestingServer provideTestingZKServer(int port) throws Exception {

        return new TestingServer(port);

    }

    public static TestingServer provideTestingZKServer() throws Exception {

        return provideTestingZKServer(DEFAULT_ZK_PORT);

    }

    public static CuratorFramework provideConnectedZKClient(String zkCluster) throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder().namespace(OMID_NAMESPACE)
                .connectString(zkCluster).retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

    public static void waitForSocketListening(String host, int port, int sleepTimeMillis)
            throws IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                Thread.sleep(sleepTimeMillis);
                continue;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            LOG.info("Host " + host + ":" + port + " is up");
            break;
        }
    }

    public static void waitForSocketNotListening(String host, int port, int sleepTimeMillis)
            throws IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                break;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            Thread.sleep(sleepTimeMillis);
            LOG.info("Host " + host + ":" + port + " is up");
        }
    }
    
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

}
