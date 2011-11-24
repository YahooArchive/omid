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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Bucket {
   
   private static final Log LOG = LogFactory.getLog(Bucket.class);
     
   private static final int BUCKET_SIZE = 32768; // 2 ^ 15

   private BitSet transactions = new BitSet(BUCKET_SIZE);
   
   private int transactionsCommited = 0;
   private int firstUncommited = 0;
   private boolean closed = false;
   private int position;

   public Bucket(int position) {
      this.position = position;
   }

   public boolean isUncommited(long id) {
      return !transactions.get((int) id % BUCKET_SIZE);
   }


   public void abortAllUncommited(Set<Long> aborted) {
      abortUncommited(BUCKET_SIZE - 1, aborted);
      closed = true;
   }

   public void abortUncommited(long id, Set<Long> aborted) {
      int lastCommited = (int) id % BUCKET_SIZE;

      if (allCommited()) {
         return;
      }

      LOG.debug("Performing scanning...");
      
      for (int i = transactions.nextClearBit(firstUncommited); i >= 0
            && i <= lastCommited; i = transactions.nextClearBit(i + 1)) {
         aborted.add(new Long(position * BUCKET_SIZE + i));
         commit(i);
      }
      
      firstUncommited = lastCommited + 1;
   }

   public void commit(long id) {
      transactions.set((int) id % BUCKET_SIZE);
      ++transactionsCommited;
   }

   public boolean allCommited() {
      return BUCKET_SIZE == transactionsCommited || closed;
   }

   public static int getBucketSize() {
      return BUCKET_SIZE;
   }

}
