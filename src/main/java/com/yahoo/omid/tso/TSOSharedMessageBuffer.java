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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;

public class TSOSharedMessageBuffer {

   private static final Log LOG = LogFactory.getLog(TSOSharedMessageBuffer.class);
   
   private TSOState state;
   
   TSOBuffer pastBuffer = new TSOBuffer();
   TSOBuffer currentBuffer = new TSOBuffer();
   ChannelBuffer writeBuffer = currentBuffer.buffer;
   Deque<TSOBuffer> futureBuffer = new ArrayDeque<TSOBuffer>();
   
   class ReadingBuffer implements Comparable<ReadingBuffer> {
       private ChannelBuffer readBuffer;
       private int readerIndex = 0;
       private TSOBuffer readingBuffer;
       private Channel channel;
       
       public ReadingBuffer(Channel channel) {
           this.channel = channel;
           this.readingBuffer = currentBuffer.reading(this);
           this.readBuffer = readingBuffer.buffer;
           this.readerIndex = readBuffer.writerIndex();
       }

       public void flush() {
          flush(true, false);
       }
       
       private void flush(boolean deleteRef, final boolean clearPast) {
          int readable = readBuffer.readableBytes() - readerIndex;

          if (readable == 0 && readingBuffer != pastBuffer) {
              if (!wrap) {
                  return;
              }
          }

          ChannelBuffer temp;
          if (wrap && readingBuffer != pastBuffer) {
              temp = ChannelBuffers.wrappedBuffer(readBuffer.slice(readerIndex, readable), tBuffer);  
          } else {
              temp = readBuffer.slice(readerIndex, readable);
          }
          ChannelFuture future = Channels.write(channel, temp);
          readerIndex += readable;
          if (readingBuffer == pastBuffer) {
             readingBuffer = currentBuffer.reading(this);
             readBuffer = readingBuffer.buffer;
             readerIndex = 0;
             readable = readBuffer.readableBytes();
             if (wrap) {
                 temp = ChannelBuffers.wrappedBuffer(readBuffer.slice(readerIndex, readable), tBuffer);
             } else {
                 temp = readBuffer.slice(readerIndex, readable);
             }
             Channels.write(channel, temp);
             readerIndex += readable;
             if (deleteRef) {
                 pastBuffer.readingBuffers.remove(this);
             }
             pastBuffer.incrementPending();
             final TSOBuffer pendingBuffer = pastBuffer;
             future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                   if (clearPast) {
                      pendingBuffer.readingBuffers.clear();
                   }
                   if (pendingBuffer.decrementPending() && pendingBuffer.readingBuffers.size() == 0) {
                      pendingBuffer.buffer.clear();
                      synchronized (futureBuffer) {
                          futureBuffer.add(pendingBuffer);
                      }
                   }
                }
             });
          }
          
       }
       
       @Override
       public int compareTo(ReadingBuffer o) {
          return this.channel.compareTo(o.channel);
       }
       
       @Override
       public boolean equals(Object obj) {
          if (!(obj instanceof ReadingBuffer))
             return false;
          ReadingBuffer buf = (ReadingBuffer) obj;
          return this.channel.equals(buf.channel);
       }

   }
   
   public TSOSharedMessageBuffer(TSOState state) {
      this.state = state;
   }

   static private final byte m0x80 = (byte) 0x80;
   static private final byte m0x3f = (byte) 0x3f;
   static private final byte m0xff = (byte) 0xff;
   
   private ChannelBuffer tBuffer;
   private boolean wrap = false;
   
   public void writeTimestamp(long timestamp) {
       wrap = true;
       tBuffer = ChannelBuffers.buffer(9);
       tBuffer.writeByte(TSOMessage.TimestampResponse);
       tBuffer.writeLong(timestamp);
   }
   
   public void rollBackTimestamp() {
       wrap = false;
   }

   public void writeCommit(long startTimestamp, long commitTimestamp) {
      if (writeBuffer.writableBytes() < 30) {
         nextBuffer();
      }
      long startDiff = startTimestamp - state.latestStartTimestamp;
      long commitDiff = commitTimestamp - state.latestCommitTimestamp;
      if (commitDiff == 1 && startDiff >= -64 && startDiff <= 63) {
        startDiff &= 0x7f;
        writeBuffer.writeByte((byte) startDiff);
    } else if (commitDiff == 1 && startDiff >= -8192 && startDiff <= 8191) {
          byte high = m0x80;
          high |= (startDiff >> 8) & m0x3f; 
          byte low = (byte) (startDiff & m0xff);
          writeBuffer.writeByte(high);
          writeBuffer.writeByte(low);
      } else if (commitDiff >= Byte.MIN_VALUE && commitDiff <= Byte.MAX_VALUE) {
          if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportByteByte);
              writeBuffer.writeByte((byte) startDiff);
          } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportShortByte);
              writeBuffer.writeShort((short) startDiff);
          } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportIntegerByte);
              writeBuffer.writeInt((int) startDiff);
          } else {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportLongByte);
              writeBuffer.writeLong((byte) startDiff);
          }
          writeBuffer.writeByte((byte) commitDiff);
      }  else if (commitDiff >= Short.MIN_VALUE && commitDiff <= Short.MAX_VALUE) {
          if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportByteShort);
              writeBuffer.writeByte((byte) startDiff);
          } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportShortShort);
              writeBuffer.writeShort((short) startDiff);
          } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportIntegerShort);
              writeBuffer.writeInt((int) startDiff);
          } else {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportLongShort);
              writeBuffer.writeLong((byte) startDiff);
          }
          writeBuffer.writeShort((short) commitDiff);
      } else {
          writeBuffer.writeByte(TSOMessage.CommittedTransactionReport);
          writeBuffer.writeLong(startTimestamp);
          writeBuffer.writeLong(commitTimestamp);
       }
      state.latestStartTimestamp = startTimestamp;
      state.latestCommitTimestamp = commitTimestamp;
   }

   public void writeHalfAbort(long startTimestamp) {
      if (writeBuffer.writableBytes() < 30) {
         nextBuffer();
      }
      long diff = startTimestamp - state.latestHalfAbortTimestamp;
      if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
          writeBuffer.writeByte(TSOMessage.AbortedTransactionReportByte);
          writeBuffer.writeByte((byte)diff);
      } else {
          writeBuffer.writeByte(TSOMessage.AbortedTransactionReport);
          writeBuffer.writeLong(startTimestamp);
      }
      
      state.latestHalfAbortTimestamp = startTimestamp;
   }
   
   public void writeFullAbort(long startTimestamp) {
      if (writeBuffer.writableBytes() < 30) {
         nextBuffer();
      }
      long diff = startTimestamp - state.latestFullAbortTimestamp;
      if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
          writeBuffer.writeByte(TSOMessage.FullAbortReportByte);
          writeBuffer.writeByte((byte)diff);
      } else {
          writeBuffer.writeByte(TSOMessage.FullAbortReport);
          writeBuffer.writeLong(startTimestamp);
      }
      
      state.latestFullAbortTimestamp = startTimestamp;
   }
   
   public void writeLargestIncrease(long largestTimestamp) {
      if (writeBuffer.writableBytes() < 30) {
         nextBuffer();
      }
      writeBuffer.writeByte(TSOMessage.LargestDeletedTimestampReport);
      writeBuffer.writeLong(largestTimestamp);
   }
   
   private void nextBuffer() {      
      LOG.debug("Switching buffers");
      Iterator<ReadingBuffer> it = pastBuffer.readingBuffers.iterator();
      boolean moreBuffers = it.hasNext();
      while(moreBuffers) {
         ReadingBuffer buf = it.next();
         moreBuffers = it.hasNext();
         buf.flush(false, !moreBuffers);
      }
      
      pastBuffer = currentBuffer;
      currentBuffer = null;
      synchronized (futureBuffer) {
         if (!futureBuffer.isEmpty()) {
             currentBuffer = futureBuffer.removeLast();
         }
      }
      if (currentBuffer == null) {
          currentBuffer = new TSOBuffer();
      }
      writeBuffer = currentBuffer.buffer;
   }
   
   public void reset() {
      if (pastBuffer != null) {
         pastBuffer.readingBuffers.clear();
         pastBuffer.buffer.clear();
      }
      if (currentBuffer != null) {
         currentBuffer.readingBuffers.clear();
         currentBuffer.buffer.clear();
      }
   }
}

