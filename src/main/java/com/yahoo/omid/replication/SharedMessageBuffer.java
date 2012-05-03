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

import com.yahoo.omid.tso.TSOMessage;

public class SharedMessageBuffer {

    private static final Log LOG = LogFactory.getLog(SharedMessageBuffer.class);

    ReferenceCountedBuffer pastBuffer = new ReferenceCountedBuffer();
    ReferenceCountedBuffer currentBuffer = new ReferenceCountedBuffer();
    ChannelBuffer writeBuffer = currentBuffer.buffer;
    Deque<ReferenceCountedBuffer> futureBuffer = new ArrayDeque<ReferenceCountedBuffer>();
    Zipper zipper = new Zipper();

    public class ReadingBuffer implements Comparable<ReadingBuffer> {
        private ChannelBuffer readBuffer;
        private int readerIndex = 0;
        private ReferenceCountedBuffer readingBuffer;
        private Channel channel;

        public ReadingBuffer(Channel channel) {
            this.channel = channel;
        }

        public void initializeIndexes() {
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
                if (wrap) {
                    Channels.write(channel, tBuffer);
                }
                return;
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
                final ReferenceCountedBuffer pendingBuffer = pastBuffer;
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

        public ZipperState getZipperState() {
            return zipper.getZipperState();
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
        zipper.encodeCommit(writeBuffer, startTimestamp, commitTimestamp);
    }

    public void writeHalfAbort(long startTimestamp) {
        if (writeBuffer.writableBytes() < 30) {
            nextBuffer();
        }
        zipper.encodeHalfAbort(writeBuffer, startTimestamp);
    }

    public void writeFullAbort(long startTimestamp) {
        if (writeBuffer.writableBytes() < 30) {
            nextBuffer();
        }
        zipper.encodeFullAbort(writeBuffer, startTimestamp);
    }

    public void writeLargestIncrease(long largestTimestamp) {
        if (writeBuffer.writableBytes() < 30) {
            nextBuffer();
        }
        zipper.encodeLargestIncrease(writeBuffer, largestTimestamp);
    }

    private void nextBuffer() {
        LOG.debug("Switching buffers");
        Iterator<ReadingBuffer> it = pastBuffer.readingBuffers.iterator();
        boolean moreBuffers = it.hasNext();
        while (moreBuffers) {
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
            currentBuffer = new ReferenceCountedBuffer();
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
