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
package com.yahoo.omid.examples.notifications;

import java.io.IOException;
import java.util.ListIterator;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.hbase.client.Put;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;

public class ExamplesUtils {

    public static class ExtendedPosixParser extends PosixParser {

        private boolean ignoreUnrecognizedOption;

        public ExtendedPosixParser(final boolean ignoreUnrecognizedOption) {
            this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
        }

        @Override
        protected void processOption(final String arg, final ListIterator iter) throws ParseException {
            boolean hasOption = getOptions().hasOption(arg);

            if (hasOption || !ignoreUnrecognizedOption) {
                super.processOption(arg, iter);
            }
        }

    }

    public static void doTransactionalPut(Transaction tx, TTable tt, byte[] rowName,
            byte[] colFamName, byte[] colName, byte[] dataValue) throws IOException {
        Put row = new Put(rowName);
        row.add(colFamName, colName, dataValue);
        tt.put(tx, row);
    }

}