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
import java.util.Set;

public class Uncommited {
   
   private static final int BKT_NUMBER = 1 << 10; // 2 ^ 10

   private Bucket buckets[] = new Bucket[BKT_NUMBER];
   private int firstUncommitedBucket = 0;
   private int lastOpenedBucket = 0;

   public Uncommited(long startTimestamp) {
      lastOpenedBucket = firstUncommitedBucket = getPosition(startTimestamp);
      commit(startTimestamp);
   }

   public synchronized void commit(long id) {
      int position = getPosition(id);
      Bucket bucket = buckets[position];
      if (bucket == null) {
         bucket = new Bucket(position);
         buckets[position] = bucket;
         lastOpenedBucket = position;
      }
      bucket.commit(id);
      if (bucket.allCommited()) {
         buckets[position] = null;
         increaseFirstUncommitedBucket();
      }
   }
   
   public void abort(long id) {
      commit(id);
   }
   
   public boolean isUncommited(long id) {
      Bucket bucket = buckets[getPosition(id)];
      if (bucket == null) {
         return false;
      }
      return bucket.isUncommited(id);
   }
   
   public synchronized Set<Long> raiseLargestDeletedTransaction(long id, Set<Long> aborted) {
      int maxBucket = getPosition(id);
      // TODO fix this iteration. When it wraps its gonna break
      for (int i = firstUncommitedBucket; i < maxBucket; ++i) {
         Bucket bucket = buckets[i];
         if (bucket != null) {
            bucket.abortAllUncommited(aborted);
            buckets[i] = null;
         }
      }
      
      Bucket bucket = buckets[maxBucket];
      if (bucket != null) {
         bucket.abortUncommited(id, aborted);
      }
      
      increaseFirstUncommitedBucket();
      
      return aborted;
   }

   private void increaseFirstUncommitedBucket() {
      while (firstUncommitedBucket != lastOpenedBucket &&
             buckets[firstUncommitedBucket] == null) {
         firstUncommitedBucket = (firstUncommitedBucket + 1) % BKT_NUMBER;
      }
   }
   
   private int getPosition(long id) {
      return ((int) (id / Bucket.getBucketSize())) % BKT_NUMBER;
   }
}
