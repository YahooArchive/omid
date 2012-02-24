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


package com.yahoo.omid.tso.persistence;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZooDefs.Ids;

import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.persistence.BookKeeperStateLogger.LedgerIdCreateCallback;
import com.yahoo.omid.tso.persistence.BookKeeperStateLogger.LoggerWatcher;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.BuilderInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerException.Code;


/**
 * Builds the TSO state from a BookKeeper ledger if there has been a previous 
 * incarnation of TSO. Note that we need to make sure that the zookeeper session 
 * is the same across builder and logger, so we create in builder and pass it
 * to logger. This is the case to prevent two TSO instances from creating a lock
 * and updating the ledger id after having lost the lock. This case constitutes
 * leads to an invalid system state.
 *
 */

public class BookKeeperStateBuilder implements StateBuilder {
    private static final Log LOG = LogFactory.getLog(BookKeeperStateBuilder.class);

    static TSOState getState(long largestDeletedTimestamp){
        return new BookKeeperStateBuilder(largestDeletedTimestamp).initialize();        
    }
    
    class Context {
        TSOState state = null;
        boolean ready = false;
        
        synchronized void setState(TSOState state){
            this.state = state;
            this.ready = true;
            this.notify();
        }
        
        synchronized boolean isReady(){
            return ready;
        }
    }
    
    long largestDeletedTimestamp;
    ZooKeeper zk;
    boolean enabled;
    
    BookKeeperStateBuilder(long largestDeletedTimestamp){
        this.largestDeletedTimestamp = largestDeletedTimestamp;
        this.zk = new ZooKeeper(System.getProperty("ZKSERVERS"), 
                                        Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                                        new LoggerWatcher()); 
        
    }
    
    class LoggerWatcher implements Watcher{
        public void process(WatchedEvent event){
             if(event.getState() != Watcher.Event.KeeperState.SyncConnected)
                 shutdown();
         }
     }
       
    
    class LockCreateCallback implements StringCallback {
        
        public void processResult(int rc, String path, Object ctx, String name){
            if(rc != Code.OK){
                LOG.warn("Failed to create znode: " + name);
                cb.builderInitComplete(Code.INITLOCKFAILED, null, ctx);
            } else {
                zk.exists(LoggerConstants.OMID_LEDGER_ID_PATH, 
                                                false,
                                                new LedgerIdReadCallback(cb),
                                                ctx);
            }         
        }
        
    }
    
    class LedgerIdReadCallback implements StatCallback {
                
        public void processResult(int rc, String path, Object ctx, Stat stat){
            if(rc == Code.OK){
                ((BookKeeperStateBuilder.Context) ctx).setState(buildStateFromLedger());
            } else {
                LOG.warn("Failed to set data. " + LoggerException.getMessage(rc)); 
                ((BookKeeperStateBuilder.Context) ctx).setState(
                                                new TSOState(new BookKeeperStateLogger(zk), 
                                                                                BookKeeperStateBuilder.this.largestDeletedTimestamp));
            }         
        }
        
    }
    
    @Override
    public TSOState initialize() {    
        /*
         * Create ZooKeeper lock
         */
              
        Context ctx = new Context();
        
        zk.create(LoggerConstants.OMID_LOCK_PATH, 
                                        new byte[0], 
                                        Ids.OPEN_ACL_UNSAFE, 
                                        CreateMode.EPHEMERAL, 
                                        new LockCreateCallback(),
                                        ctx);
        
        synchronized(ctx){
            int counter = 0;
            while(!ctx.isReady() || (counter > 10)){
                ctx.wait(1000);
                counter++;
            }
            
        }
        
        return ctx.state;
    }

    /**
     * Build the state from a
     * 
     * @param cb
     * @param ctx
     */
    private TSOState buildStateFromLedger(){
        /*
         * TODO: Build state
         */
        
        TSOState state = new TSOState(new BookKeeperStateLogger(this.zk), this.largestDeletedTimestamp); 
     
        return state; 
    }
    
    /**
     * Disables this builder.    
     */
    public void shutdown(){
        this.enabled = false;
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            LOG.error(e);
        } 
    }
}
