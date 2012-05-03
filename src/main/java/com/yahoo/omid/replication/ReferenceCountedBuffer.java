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

package com.yahoo.omid.replication;

import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;


public class ReferenceCountedBuffer {
   private static final Log LOG = LogFactory.getLog(ReferenceCountedBuffer.class);

   private static final int CAPACITY = 1024*1024;
   
   public ChannelBuffer buffer;
   public TreeSet<SharedMessageBuffer.ReadingBuffer> readingBuffers = new TreeSet<SharedMessageBuffer.ReadingBuffer>();
   private int pendingWrites = 0;
   
   public static long nBuffers;
   
   
   public ReferenceCountedBuffer() {
      this(true);
   }
   
   public ReferenceCountedBuffer(boolean allocateBuffer) {
      nBuffers++;
      LOG.warn("Allocated buffer");
      if (allocateBuffer)
         buffer = ChannelBuffers.directBuffer(CAPACITY);
   }
   
   public ReferenceCountedBuffer reading(SharedMessageBuffer.ReadingBuffer buf) {
      readingBuffers.add(buf);
      return this;
   }
   
   public synchronized void incrementPending() {
      ++pendingWrites;
   }
   
   public synchronized boolean decrementPending() {
      return --pendingWrites == 0;
   }
}
