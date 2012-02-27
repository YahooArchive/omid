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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.ZooDefs.Ids;

import com.sun.tools.javac.util.Log;
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

    static TSOState getState(long largestDeletedTimestamp){
        return new BookKeeperStateBuilder(largestDeletedTimestamp).initialize();        
    }
        
    long largestDeletedTimestamp;
    ZooKeeper zk;
    LoggerProtocol lp;
    boolean enabled;
    
    BookKeeperStateBuilder(long largestDeletedTimestamp){
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
            } else {
                LOG.warn("Failed to set data. " + LoggerException.getMessage(rc)); 
                ((BookKeeperStateBuilder.Context) ctx).setState(
                                                new TSOState(new BookKeeperStateLogger(zk), 
                                                                                BookKeeperStateBuilder.this.largestDeletedTimestamp));
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
                    ByteBuffer bb = ByteBuffer.wrap(le.getEntry());
                    boolean done = false;
                    
                    while(!done){
                        byte id = bb.get();
                        long value = bb.getLong();
                        lp.execute(id, value);
                        done = !bb.hasRemaining();
                    }
                    
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
        this.lp = new LoggerProtocol(new BookKeeperStateLogger(this.zk), this.largestDeletedTimestamp); 
        
        
        /*
         * Open ledger for reading.
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(), this.zk);    
        ByteBuffer bb = ByteBuffer.wrap(data);
        bk.asyncOpenLedger(bb.getLong(), BookKeeper.DigestType.CRC32, 
                                        "flavio was here".getBytes(), 
                                        new OpenCallback(){
            public void openComplete(int rc, LedgerHandle lh, Object ctx){
                if(rc != BKException.Code.OK){
                    LOG.error("Could not open ledger for reading." + BKException.getMessage(rc));
                    ((BookKeeperStateBuilder.Context) ctx).setState(null);
                } else {
                    long last = lh.getLastAddConfirmed();
                    long counter = 0;
                    while(counter < last){
                        long nextBatch = Math.min(counter + 1000, last);
                        lh.asyncReadEntries(counter, nextBatch, new LoggerExecutor(), ctx);
                    }
                }   
            }
        }, ctx);
        
        /*
         * Wait until operation completes.
         */
        
        synchronized(ctx){
            while(!((Context) ctx).isReady()){
                ctx.wait();
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
            LOG.error(e);
        } 
    }
}
