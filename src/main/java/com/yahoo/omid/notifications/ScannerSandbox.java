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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScannerSandbox {

    private static final Log logger = LogFactory.getLog(ScannerSandbox.class);

    private AppSandbox appSandbox;

    // Key: Interest where the scanners running on the ScannerContainer will do
    // their work
    // Value: The ScannerContainer that executes the scanner threads scanning
    // each particular interest
    private ConcurrentHashMap<String, ScannerContainer> scanners = new ConcurrentHashMap<String, ScannerContainer>();


    public ScannerSandbox(AppSandbox appSandbox) {
        this.appSandbox = appSandbox;
    }

    public void addScannerContainer(String interest) throws Exception {
        ScannerContainer scannerContainer = scanners.get(interest);
        if (scannerContainer == null) {
            scannerContainer = new ScannerContainer(interest, appSandbox);
            scannerContainer.start();
            scanners.putIfAbsent(interest, scannerContainer);
            logger.trace("ScannerContainer created for interest " + interest);
        }
    }

    /**
     * @param interest
     * @throws InterruptedException
     */
    public void removeScannerContainer(String interest) throws InterruptedException {
        ScannerContainer scannerContainer = scanners.get(interest);
        scannerContainer.stop();
        scanners.remove(interest);
        logger.trace("ScannerContainer stopped and removed for interest " + interest);
    }

}
