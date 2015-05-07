package com.yahoo.omid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class contains functionality that is useful for the Omid tests.
 * 
 */
public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static void waitForSocketListening(String host, int port,
            int sleepTimeMillis) throws UnknownHostException, IOException,
            InterruptedException {
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

    public static void waitForSocketNotListening(String host, int port,
            int sleepTimeMillis) throws UnknownHostException, IOException,
            InterruptedException {
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
