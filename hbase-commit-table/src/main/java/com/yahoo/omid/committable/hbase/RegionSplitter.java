/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.committable.hbase;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

/**
 * This class contains only the required behavior of the original
 * org.apache.hadoop.hbase.util.RegionSplitter class to avoid
 * having a reference to hbase-testing-util, which transitively
 * imports hbase-server causing dependency conflicts for this module.
 */
public class RegionSplitter {

    /**
     * A generic interface for the RegionSplitter code to use for all it's functionality. Note that the original authors
     * of this code use {@link HexStringSplit} to partition their table and set it as default, but provided this for
     * your custom algorithm. To use, create a new derived class from this interface and call
     * {@link RegionSplitter#createPresplitTable} or
     * {@link RegionSplitter#rollingSplit(String, SplitAlgorithm, Configuration)} with the argument splitClassName
     * giving the name of your class.
     */
    public interface SplitAlgorithm {
        /**
         * Split a pre-existing region into 2 regions.
         * 
         * @param start
         *            first row (inclusive)
         * @param end
         *            last row (exclusive)
         * @return the split row to use
         */
        byte[] split(byte[] start, byte[] end);

        /**
         * Split an entire table.
         * 
         * @param numRegions
         *            number of regions to split the table into
         * 
         * @throws RuntimeException
         *             user input is validated at this time. may throw a runtime exception in response to a parse
         *             failure
         * @return array of split keys for the initial regions of the table. The length of the returned array should be
         *         numRegions-1.
         */
        byte[][] split(int numRegions);

        /**
         * In HBase, the first row is represented by an empty byte array. This might cause problems with your split
         * algorithm or row printing. All your APIs will be passed firstRow() instead of empty array.
         * 
         * @return your representation of your first row
         */
        byte[] firstRow();

        /**
         * In HBase, the last row is represented by an empty byte array. This might cause problems with your split
         * algorithm or row printing. All your APIs will be passed firstRow() instead of empty array.
         * 
         * @return your representation of your last row
         */
        byte[] lastRow();

        /**
         * In HBase, the last row is represented by an empty byte array. Set this value to help the split code
         * understand how to evenly divide the first region.
         * 
         * @param userInput
         *            raw user input (may throw RuntimeException on parse failure)
         */
        void setFirstRow(String userInput);

        /**
         * In HBase, the last row is represented by an empty byte array. Set this value to help the split code
         * understand how to evenly divide the last region. Note that this last row is inclusive for all rows sharing
         * the same prefix.
         * 
         * @param userInput
         *            raw user input (may throw RuntimeException on parse failure)
         */
        void setLastRow(String userInput);

        /**
         * @param input
         *            user or file input for row
         * @return byte array representation of this row for HBase
         */
        byte[] strToRow(String input);

        /**
         * @param row
         *            byte array representing a row in HBase
         * @return String to use for debug & file printing
         */
        String rowToStr(byte[] row);

        /**
         * @return the separator character to use when storing / printing the row
         */
        String separator();

        /**
         * Set the first row
         * 
         * @param userInput
         *            byte array of the row key.
         */
        void setFirstRow(byte[] userInput);

        /**
         * Set the last row
         * 
         * @param userInput
         *            byte array of the row key.
         */
        void setLastRow(byte[] userInput);
    }

    /**
     * @throws IOException
     *             if the specified SplitAlgorithm class couldn't be instantiated
     */
    public static SplitAlgorithm newSplitAlgoInstance(Configuration conf,
            String splitClassName) throws IOException {
        Class<?> splitClass;

        // For split algorithms builtin to RegionSplitter, the user can specify
        // their simple class name instead of a fully qualified class name.
        if (splitClassName.equals(UniformSplit.class.getSimpleName())) {
            splitClass = UniformSplit.class;
        } else {
            try {
                splitClass = conf.getClassByName(splitClassName);
            } catch (ClassNotFoundException e) {
                throw new IOException("Couldn't load split class " + splitClassName, e);
            }
            if (splitClass == null) {
                throw new IOException("Failed loading split class " + splitClassName);
            }
            if (!SplitAlgorithm.class.isAssignableFrom(splitClass)) {
                throw new IOException(
                        "Specified split class doesn't implement SplitAlgorithm");
            }
        }
        try {
            return splitClass.asSubclass(SplitAlgorithm.class).newInstance();
        } catch (Exception e) {
            throw new IOException("Problem loading split algorithm: ", e);
        }
    }

    /**
     * A SplitAlgorithm that divides the space of possible keys evenly. Useful when the keys are approximately uniform
     * random bytes (e.g. hashes). Rows are raw byte values in the range <b>00 => FF</b> and are right-padded with zeros
     * to keep the same memcmp() order. This is the natural algorithm to use for a byte[] environment and saves space,
     * but is not necessarily the easiest for readability.
     */
    public static class UniformSplit implements SplitAlgorithm {
        static final byte xFF = (byte) 0xFF;
        byte[] firstRowBytes = ArrayUtils.EMPTY_BYTE_ARRAY;
        byte[] lastRowBytes =
                new byte[] { xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF };

        public byte[] split(byte[] start, byte[] end) {
            return Bytes.split(start, end, 1)[1];
        }

        @Override
        public byte[][] split(int numRegions) {
            Preconditions.checkArgument(
                    Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
                    "last row (%s) is configured less than first row (%s)",
                    Bytes.toStringBinary(lastRowBytes),
                    Bytes.toStringBinary(firstRowBytes));

            byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes, true,
                    numRegions - 1);
            Preconditions.checkState(splits != null,
                    "Could not split region with given user input: " + this);

            // remove endpoints, which are included in the splits list
            return Arrays.copyOfRange(splits, 1, splits.length - 1);
        }

        @Override
        public byte[] firstRow() {
            return firstRowBytes;
        }

        @Override
        public byte[] lastRow() {
            return lastRowBytes;
        }

        @Override
        public void setFirstRow(String userInput) {
            firstRowBytes = Bytes.toBytesBinary(userInput);
        }

        @Override
        public void setLastRow(String userInput) {
            lastRowBytes = Bytes.toBytesBinary(userInput);
        }

        @Override
        public void setFirstRow(byte[] userInput) {
            firstRowBytes = userInput;
        }

        @Override
        public void setLastRow(byte[] userInput) {
            lastRowBytes = userInput;
        }

        @Override
        public byte[] strToRow(String input) {
            return Bytes.toBytesBinary(input);
        }

        @Override
        public String rowToStr(byte[] row) {
            return Bytes.toStringBinary(row);
        }

        @Override
        public String separator() {
            return ",";
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
                    + "," + rowToStr(lastRow()) + "]";
        }
    }
}
