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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

public class NotificationServer {

    private static final Logger logger = Logger.getLogger(NotificationServer.class);
    
    /**
     * This is where all starts...
     * 
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        final NotificationZKWatchdog service = new NotificationZKWatchdog();
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {                
                service.stopAndWait();
                logger.info("ooo Omid ooo - Notification Server stopped - ooo Omid ooo");
            }
         });
        
        logger.info("ooo Omid ooo - Starting Notification Server - ooo Omid ooo");
        service.startAndWait();
        
        synchronized(service) {
            try {
                service.wait();
            } finally {
                service.stopAndWait();
                logger.info("ooo Omid ooo - Notification Server stopped - ooo Omid ooo");
            }
        }
    }
    
    public static boolean waitForServerUp(String targetHostPort, long timeout) {
        long start = System.currentTimeMillis();
        String split[] = targetHostPort.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader =
                        new BufferedReader(
                                new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        logger.info("Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                logger.info("Server " + targetHostPort + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

}
