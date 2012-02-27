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

package com.yahoo.omid.tso;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.yahoo.omid.tso.persistence.StateLogger;


/**
 * The wrapper for different states of TSO
 * This state is shared between handlers
 * @author maysam
 */
public class TSOState {
   /**
    * The maximum entries kept in TSO
    */
   // static final public int MAX_ITEMS = 10000000;
   // static final public int MAX_ITEMS = 4000000;
   static public int MAX_ITEMS = 100;
   static {
      try {
         MAX_ITEMS = Integer.valueOf(System.getProperty("omid.maxItems"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   static public int MAX_COMMITS = 100;
   static {
      try {
         MAX_COMMITS = Integer.valueOf(System.getProperty("omid.maxCommits"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   static public int FLUSH_TIMEOUT = 5;
   static {
      try {
         FLUSH_TIMEOUT = Integer.valueOf(System.getProperty("omid.flushTimeout"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   /**
    * Hash map load factor
    */
   static final public float LOAD_FACTOR = 0.5f;
   
   /**
    * Object that implements the logic to log records
    * for recoverability
    */
   
   StateLogger logger;

   public StateLogger getLogger(){
       return logger;
   }
   
   
   /**
    * Largest Deleted Timestamp
    */
   public long largestDeletedTimestamp = 0;
   public long previousLargestDeletedTimestamp = 0;
   public long latestCommitTimestamp = 0;
   public long latestStartTimestamp = 0;
   public long latestHalfAbortTimestamp = 0;
   public long latestFullAbortTimestamp = 0;
   
   public TSOSharedMessageBuffer sharedMessageBuffer = new TSOSharedMessageBuffer(this);

   /**
    * The hash map to to keep track of recently committed rows
    * each bucket is about 20 byte, so the initial capacity is 20MB
    */
   public CommitHashMap hashmap = new CommitHashMap(MAX_ITEMS, LOAD_FACTOR);

   public Uncommited uncommited;

   /**
    * Process commit request.
    * 
    * @param startTimestamp
    */
   protected void processCommit(long startTimestamp){
       //TODO: Process commit;
   }
   
   /**
    * Process abort request.
    * 
    * @param startTimestamp
    */
   protected void processAbort(long startTimestamp){
       hashmap.setHalfAborted(startTimestamp);
       uncommited.abort(startTimestamp);
   }
   
   /**
    * Process full abort report.
    * 
    * @param startTimestamp
    */
   protected void processFullAbort(long startTimestamp){
       hashmap.setFullAborted(startTimestamp);
   }

   /*
    * WAL related pointers
    */
   public static int BATCH_SIZE = 0;//in bytes
   public ByteArrayOutputStream baos = new ByteArrayOutputStream();
   public DataOutputStream toWAL = new DataOutputStream(baos);
   public List<TSOHandler.ChannelandMessage> nextBatch = new ArrayList<TSOHandler.ChannelandMessage>();
   
   public TSOState(StateLogger logger, long largestDeletedTimestamp) {
      this.largestDeletedTimestamp = this.previousLargestDeletedTimestamp = largestDeletedTimestamp;
      this.uncommited = new Uncommited(largestDeletedTimestamp);
      this.logger = logger;
   }
}

