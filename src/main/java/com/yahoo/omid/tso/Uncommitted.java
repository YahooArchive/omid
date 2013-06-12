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
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Uncommitted {
   private static final Logger LOG = LoggerFactory.getLogger(TSOHandler.class);
   
   private final long bucketNumber;
   private final long bucketSize;

   private Bucket buckets[];
   private int firstUncommitedBucket = 0;
   private long firstUncommitedAbsolute = 0;
   private int lastOpenedBucket = 0;

   public Uncommitted(long startTimestamp, int bucketNumber, int bucketSize) {
      this.bucketSize = bucketSize;
      this.bucketNumber = bucketNumber;
      this.buckets = new Bucket[(int) bucketNumber];
      lastOpenedBucket = firstUncommitedBucket = getRelativePosition(startTimestamp);
      firstUncommitedAbsolute = getAbsolutePosition(startTimestamp);
      long ts = startTimestamp & ~(bucketSize - 1);
      LOG.info("Start TS : " + startTimestamp + " firstUncom: " + firstUncommitedBucket + " Mask:" + ts );
      LOG.info("BKT_NUMBER : " + bucketNumber + " BKT_SIZE: " + bucketSize);
      for (; ts <= startTimestamp; ++ts)
         commit(ts);
   }

   public synchronized void commit(long id) {
      int position = getRelativePosition(id);
      Bucket bucket = buckets[position];
      if (bucket == null) {
         bucket = new Bucket(getAbsolutePosition(id), (int) bucketSize);
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
      Bucket bucket = buckets[getRelativePosition(id)];
      if (bucket == null) {
         return false;
      }
      return bucket.isUncommited(id);
   }
   
   public Set<Long> raiseLargestDeletedTransaction(long id) {
      if (firstUncommitedAbsolute > getAbsolutePosition(id))
         return Collections.emptySet();
      int maxBucket = getRelativePosition(id);
      Set<Long> aborted = new TreeSet<Long>();
      for (int i = firstUncommitedBucket; i != maxBucket ; i = (int)((i+1) % bucketNumber)) {
         Bucket bucket = buckets[i];
         if (bucket != null) {
            aborted.addAll(bucket.abortAllUncommited());
            buckets[i] = null;
         }
      }
      
      Bucket bucket = buckets[maxBucket];
      if (bucket != null) {
         aborted.addAll(bucket.abortUncommited(id));
      }
      
      increaseFirstUncommitedBucket();
      
      return aborted;
   }
   
   public synchronized long getFirstUncommitted() {
      return buckets[firstUncommitedBucket].getFirstUncommitted();
   }

   private synchronized void increaseFirstUncommitedBucket() {
      while (firstUncommitedBucket != lastOpenedBucket &&
             buckets[firstUncommitedBucket] == null) {
         firstUncommitedBucket = (int)((firstUncommitedBucket + 1) % bucketNumber);
         firstUncommitedAbsolute++;
      }
   }

   private int getRelativePosition(long id) {
      return (int) ((id / bucketSize) % bucketNumber);
   }

   private long getAbsolutePosition(long id) {
      return id / bucketSize;
   }
}
