/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.committable.hbase;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;

/**
 * Contains implementations of the KeyGenerator interface
 */
public class KeyGeneratorImplementations {

    public static KeyGenerator defaultKeyGenerator() {
        return new BucketKeyGenerator();
    }

    /**
     * This is the used implementation
     */
    public static class BucketKeyGenerator implements KeyGenerator {

        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            byte[] bytes = new byte[9];
            bytes[0] = (byte) (startTimestamp & 0x0F);
            bytes[1] = (byte) ((startTimestamp >> 56) & 0xFF);
            bytes[2] = (byte) ((startTimestamp >> 48) & 0xFF);
            bytes[3] = (byte) ((startTimestamp >> 40) & 0xFF);
            bytes[4] = (byte) ((startTimestamp >> 32) & 0xFF);
            bytes[5] = (byte) ((startTimestamp >> 24) & 0xFF);
            bytes[6] = (byte) ((startTimestamp >> 16) & 0xFF);
            bytes[7] = (byte) ((startTimestamp >> 8) & 0xFF);
            bytes[8] = (byte) ((startTimestamp) & 0xFF);
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) {
            assert (key.length == 9);
            return ((long) key[1] & 0xFF) << 56
                    | ((long) key[2] & 0xFF) << 48
                    | ((long) key[3] & 0xFF) << 40
                    | ((long) key[4] & 0xFF) << 32
                    | ((long) key[5] & 0xFF) << 24
                    | ((long) key[6] & 0xFF) << 16
                    | ((long) key[7] & 0xFF) << 8
                    | ((long) key[8] & 0xFF);
        }

    }

    public static class FullRandomKeyGenerator implements KeyGenerator {

        @Override
        public byte[] startTimestampToKey(long startTimestamp) {
            assert startTimestamp >= 0;
            // 1) Reverse the timestamp to better spread
            long reversedStartTimestamp = Long.reverse(startTimestamp | Long.MIN_VALUE);
            // 2) Convert to a byte array with big endian format
            byte[] bytes = new byte[8];
            bytes[0] = (byte) ((reversedStartTimestamp >> 56) & 0xFF);
            bytes[1] = (byte) ((reversedStartTimestamp >> 48) & 0xFF);
            bytes[2] = (byte) ((reversedStartTimestamp >> 40) & 0xFF);
            bytes[3] = (byte) ((reversedStartTimestamp >> 32) & 0xFF);
            bytes[4] = (byte) ((reversedStartTimestamp >> 24) & 0xFF);
            bytes[5] = (byte) ((reversedStartTimestamp >> 16) & 0xFF);
            bytes[6] = (byte) ((reversedStartTimestamp >> 8) & 0xFF);
            bytes[7] = (byte) ((reversedStartTimestamp) & 0xFE);
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) {
            assert (key.length == 8);
            // 1) Convert from big endian each byte
            long startTimestamp = ((long) key[0] & 0xFF) << 56
                    | ((long) key[1] & 0xFF) << 48
                    | ((long) key[2] & 0xFF) << 40
                    | ((long) key[3] & 0xFF) << 32
                    | ((long) key[4] & 0xFF) << 24
                    | ((long) key[5] & 0xFF) << 16
                    | ((long) key[6] & 0xFF) << 8
                    | ((long) key[7] & 0xFF);
            // 2) Reverse to obtain the original value
            return Long.reverse(startTimestamp);
        }
    }

    public static class BadRandomKeyGenerator implements KeyGenerator {

        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            long reversedStartTimestamp = Long.reverse(startTimestamp);
            byte[] bytes = new byte[CodedOutputStream.computeSFixed64SizeNoTag(reversedStartTimestamp)];
            CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
            cos.writeSFixed64NoTag(reversedStartTimestamp);
            cos.flush();
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) throws IOException {
            CodedInputStream cis = CodedInputStream.newInstance(key);
            return Long.reverse(cis.readSFixed64());
        }

    }

    public static class SeqKeyGenerator implements KeyGenerator {

        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            // Convert to a byte array with big endian format
            byte[] bytes = new byte[8];

            bytes[0] = (byte) ((startTimestamp >> 56) & 0xFF);
            bytes[1] = (byte) ((startTimestamp >> 48) & 0xFF);
            bytes[2] = (byte) ((startTimestamp >> 40) & 0xFF);
            bytes[3] = (byte) ((startTimestamp >> 32) & 0xFF);
            bytes[4] = (byte) ((startTimestamp >> 24) & 0xFF);
            bytes[5] = (byte) ((startTimestamp >> 16) & 0xFF);
            bytes[6] = (byte) ((startTimestamp >> 8) & 0xFF);
            bytes[7] = (byte) ((startTimestamp) & 0xFF);
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) {
            assert (key.length == 8);
            // Convert from big endian each byte
            return ((long) key[0] & 0xFF) << 56
                    | ((long) key[1] & 0xFF) << 48
                    | ((long) key[2] & 0xFF) << 40
                    | ((long) key[3] & 0xFF) << 32
                    | ((long) key[4] & 0xFF) << 24
                    | ((long) key[5] & 0xFF) << 16
                    | ((long) key[6] & 0xFF) << 8
                    | ((long) key[7] & 0xFF);
        }

    }

}
