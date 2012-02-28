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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class BookKeeperStateBuilder extends StateBuilder {
    private static final Log LOG = LogFactory.getLog(BookKeeperStateBuilder.class);
    
    /*
     * Assuming that each entry is 1k bytes, we read 50k bytes at each call.
     * It provides a good degree of parallelism.
     */
    private static final long BKREADBATCHSIZE = 50;

    public static TSOState getState(long largestDeletedTimestamp){
        TSOState returnValue;
        if(System.getProperty("ZKSERVERS") == null){
            LOG.warn("Logger is disabled");
            returnValue = new TSOState(largestDeletedTimestamp);
        } else {
            try{
                returnValue = new BookKeeperStateBuilder(largestDeletedTimestamp).initialize();
            } catch (Throwable e) {
                LOG.error("Error while building the state.", e);
                returnValue = null;
            }
        }
        return returnValue;        
    }
        
    long largestDeletedTimestamp;
    ZooKeeper zk;
    LoggerProtocol lp;
    boolean enabled;
    
    BookKeeperStateBuilder(long largestDeletedTimestamp) 
    throws IOException, InterruptedException, KeeperException{
        this.largestDeletedTimestamp = largestDeletedTimestamp;
        this.zk = new ZooKeeper(System.getProperty("ZKSERVERS"), 
                                        Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                                        new LoggerWatcher()); 
        
    }

    /**
     * Context objects for callbacks.
     *
     */
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
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            } else {
                zk.getData(LoggerConstants.OMID_LEDGER_ID_PATH, 
                                                false,
                                                new LedgerIdReadCallback(),
                                                ctx);
            }         
        }
        
    }
    
    class LedgerIdReadCallback implements DataCallback {
                
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            if(rc == Code.OK){
                buildStateFromLedger(data, ctx);
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                LOG.warn("Failed to read data. " + LoggerException.getMessage(rc)); 
                TSOState tempState; 
                try{
                    tempState = new TSOState(new BookKeeperStateLogger(zk), 
                                                    BookKeeperStateBuilder.this.largestDeletedTimestamp);
                } catch (Exception e) {
                    LOG.error("Error while creating state logger.", e);
                    tempState = null;
                }
                ((BookKeeperStateBuilder.Context) ctx).setState(tempState);                                                
            } else {
                LOG.warn("Failed to read data. " + LoggerException.getMessage(rc));
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            }
        }
    }
    
    /**
     * Invoked after the execution of a ledger read. Instances are
     * created in the open callback. 
     *
     */
    class LoggerExecutor implements ReadCallback {
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx){
            if(rc != BKException.Code.OK){
                LOG.error("Error while reading ledger entries." + BKException.getMessage(rc));
                ((BookKeeperStateBuilder.Context) ctx).setState(null);
            } else {
                while(entries.hasMoreElements()){
                    LedgerEntry le = entries.nextElement();
                    lp.execute(ByteBuffer.wrap(le.getEntry()));
                                       
                    if(le.getEntryId() == lh.getLastAddConfirmed()){
                        ((BookKeeperStateBuilder.Context) ctx).setState(lp.getState());
                    }
                }
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
        try{
            synchronized(ctx){
                int counter = 0;
                while(!ctx.isReady() || (counter > 10)){
                    ctx.wait(1000);
                    counter++;
                }
            
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for state to build up.", e);
            ctx.setState(null);
        }
        
        return ctx.state;
    }


    /**
     * Builds state from a ledger.
     * 
     * 
     * @param data
     * @param ctx
     * @return
     */
    private TSOState buildStateFromLedger(byte[] data, Object ctx){
        if(data == null){
            LOG.error("No data on znode, can't determine ledger id");
            ((BookKeeperStateBuilder.Context) ctx).setState(null);
        }
        
       /*
        * Instantiates LoggerProtocol        
        */
        try{
            this.lp = new LoggerProtocol(new BookKeeperStateLogger(this.zk), this.largestDeletedTimestamp); 
        } catch (Exception e) {
            LOG.error("Error while creating state logger for logger protocol.", e);
            return null;
        }
        
        /*
         * Open ledger for reading.
         */

        BookKeeper bk;
        try{
            bk = new BookKeeper(new ClientConfiguration(), this.zk);    
        } catch (Exception e) {
            LOG.error("Error while creating bookkeeper object", e);
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        bk.asyncOpenLedger(bb.getLong(), BookKeeper.DigestType.CRC32, 
                                        "flavio was here".getBytes(), 
                                        new OpenCallback(){
            public void openComplete(int rc, LedgerHandle lh, Object ctx){
                if(rc != BKException.Code.OK){
                    LOG.error("Could not open ledger for reading." + BKException.getMessage(rc));
                    ((BookKeeperStateBuilder.Context) ctx).setState(null);
                } else {
                    long counter = lh.getLastAddConfirmed();
                    while(counter > 0){
                        long nextBatch = Math.max(counter - BKREADBATCHSIZE, 0);
                        lh.asyncReadEntries(nextBatch, counter, new LoggerExecutor(), ctx);
                        counter -= BKREADBATCHSIZE;
                    }
                }   
            }
        }, ctx);
        
        /*
         * Wait until operation completes.
         */
        
        synchronized(ctx){
            try{
                while(!((Context) ctx).isReady()){
                    ctx.wait();
                }
            } catch (Exception e) {
                LOG.error("Error while creating bookkeeper object", e);
                return null;
            }
        }
        
        return lp.getState(); 
    }
    
    /**
     * Disables this builder.    
     */
    public void shutdown(){
        this.enabled = false;
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            LOG.error("Error while shutting down", e);
        } 
    }
}
