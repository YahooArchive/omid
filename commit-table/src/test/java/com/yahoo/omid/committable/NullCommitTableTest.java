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

    @Test
    public void testClientAndWriter() throws Exception {

        CommitTable commitTable = new NullCommitTable();

        try (CommitTable.Client commitTableClient = commitTable.getClient().get();
             CommitTable.Writer commitTableWriter = commitTable.getWriter().get()) {

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
            assertNull(commitTableWriter.flush().get());
        }
    }

}
