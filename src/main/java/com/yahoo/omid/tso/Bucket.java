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
import java.util.BitSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Bucket {
   
   private static final Log LOG = LogFactory.getLog(Bucket.class);
     
   private final long bucketSize;
   private final BitSet transactions;
   
   private int transactionsCommited = 0;
   private int firstUncommited = 0;
   private boolean closed = false;
   private final long position;

   public Bucket(long position, int bucketSize) {
      this.position = position;
      this.bucketSize = bucketSize;
      this.transactions = new BitSet(bucketSize);
   }

   public boolean isUncommited(long id) {
      return !transactions.get((int) (id % bucketSize));
   }


   public Set<Long> abortAllUncommited() {
      Set<Long> result = abortUncommited(bucketSize - 1);
      closed = true;
      return result;
   }

   public synchronized Set<Long> abortUncommited(long id) {
      int lastCommited = (int) (id % bucketSize);
      
      Set<Long> aborted = new TreeSet<Long>();
      if (allCommited()) {
         return aborted;
      }

      LOG.trace("Performing scanning...");
      
      for (int i = transactions.nextClearBit(firstUncommited); i >= 0
            && i <= lastCommited; i = transactions.nextClearBit(i + 1)) {
         aborted.add(position * bucketSize + i);
         commit(i);
      }
      
      firstUncommited = lastCommited + 1;

      return aborted;
   }

   public synchronized void commit(long id) {
      transactions.set((int) (id % bucketSize));
      ++transactionsCommited;
   }

   public boolean allCommited() {
      return bucketSize == transactionsCommited || closed;
   }

   public long getBucketSize() {
      return bucketSize;
   }

   public long getFirstUncommitted() {
      return position * bucketSize + firstUncommited;
   }

}
