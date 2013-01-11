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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScannerManager {

    private static final Log logger = LogFactory.getLog(ScannerManager.class);
    
    private Map<String, List<String>> interestsToObservers;
    private Map<String, List<String>> observersToHosts;

    // Key: ScannerContainer name representing the table where the scanner container is performing scanning
    // Value: The scanner container that executes the scanner threads
    private Map<String, ScannerContainer> scanners = new HashMap<String, ScannerContainer>();
    
    public ScannerManager(Map<String, List<String>> interestsToObservers, Map<String, List<String>> observersToHosts) {
        this.interestsToObservers = interestsToObservers;
        this.observersToHosts = observersToHosts;
    }
    
    public void addScannerContainer(String interest) throws Exception {
        ScannerContainer scannerContainer = scanners.get(interest);
        if(scannerContainer == null) {
            scannerContainer = new ScannerContainer(interest, interestsToObservers, observersToHosts);
            scannerContainer.start();
            scanners.put(interest, scannerContainer);
            logger.trace("ScannerContainer created for interest " + interest);
        }
    }
    
    /**
     * @param node
     * @throws InterruptedException 
     */
    public void removeScannerContainer(String node) throws InterruptedException {
        ScannerContainer scannerContainer = scanners.get(node);
        scannerContainer.stop();
        scanners.remove(node);        
    }
    
}
