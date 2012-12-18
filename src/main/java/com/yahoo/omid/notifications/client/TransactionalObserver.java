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
package com.yahoo.omid.notifications.client;

import org.apache.log4j.Logger;

import com.yahoo.omid.client.TransactionState;

public class TransactionalObserver {
    
    private static final Logger logger = Logger.getLogger(TransactionalObserver.class);
    
    private String name; // The name of the observer
    private ObserverBehaviour observer;

    public TransactionalObserver(String name, ObserverBehaviour observer) {
        this.name = name;
        this.observer = observer;
    }
    
    /**
     * @return the transactional observer's name
     */
    public String getName() {
        return name;
    }

    public void notify(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
        startTx();
        observer.updated(table, rowKey, columnFamily, column);
        commitTx();        
    }

    /**
     * 
     */
    private void startTx() {
        logger.trace("Starting a new transaction");
        
    }
    
    /**
     * 
     */
    private void commitTx() {
        logger.trace("Commiting transaction");
        
    }

    /**
     * TODO Remove if delegation is implemented. Only valid if this is implemented as an abstract class
     */
    public TransactionState getTx() {
        return null;   
    }
    
}
