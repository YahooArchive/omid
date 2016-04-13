/**
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
package com.yahoo.omid.committable;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * TODO: Remove this class when removing this class from production code
 */
public class NullCommitTableTest {

    private static final long TEST_ST = 1L;
    private static final long TEST_CT = 2L;
    private static final long TEST_LWM = 1L;

    @Test(timeOut = 10_000)
    public void testClientAndWriter() throws Exception {

        CommitTable commitTable = new NullCommitTable();

        try (CommitTable.Client commitTableClient = commitTable.getClient();
             CommitTable.Writer commitTableWriter = commitTable.getWriter()) {

            // Test client
            try {
                commitTableClient.readLowWatermark().get();
            } catch (UnsupportedOperationException e) {
                // expected
            }

            try {
                commitTableClient.getCommitTimestamp(TEST_ST).get();
            } catch (UnsupportedOperationException e) {
                // expected
            }

            try {
                commitTableClient.tryInvalidateTransaction(TEST_ST).get();
            } catch (UnsupportedOperationException e) {
                // expected
            }

            assertNull(commitTableClient.completeTransaction(TEST_ST).get());

            // Test writer
            commitTableWriter.updateLowWatermark(TEST_LWM);
            commitTableWriter.addCommittedTransaction(TEST_ST, TEST_CT);
            commitTableWriter.clearWriteBuffer();
            commitTableWriter.flush();
        }
    }

}
