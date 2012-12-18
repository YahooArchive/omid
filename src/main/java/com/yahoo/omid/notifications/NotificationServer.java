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

import org.apache.log4j.Logger;

public class NotificationServer {

    private static final Logger logger = Logger.getLogger(NotificationServer.class);
    
    /**
     * This is where all starts...
     * 
     * @param args
     */
    public static void main(String[] args) {
        final ScannerManager service = new ScannerManager();
        
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
            } catch (InterruptedException e) {
                // do what's in finally
            } finally {
                service.stopAndWait();
                logger.info("ooo Omid ooo - Notification Server stopped - ooo Omid ooo");
            }
        }
    }

}
