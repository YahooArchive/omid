package com.yahoo.omid.transaction;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import java.util.Arrays;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

public class TestShadowCellsUpgrade extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestShadowCells.class);

    static final byte[] row1 = Bytes.toBytes("test-sc1");
    static final byte[] row2 = Bytes.toBytes("test-sc2");
    static final byte[] row3 = Bytes.toBytes("test-sc3");
    static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    static final byte[] qualifier = Bytes.toBytes("testdata");
    static final byte[] data1 = Bytes.toBytes("testWrite-1");
    static final byte[] data2 = Bytes.toBytes("testWrite-2");

    // TODO: After removing the legacy shadow cell suffix, maybe we should
    // mix the assertions in this test with the ones in TestShadowCells in
    // a further commit
    /**
     * Test that the new client can read shadow cells written by the old
     * client.
     */
    @Test
    public void testGetOldShadowCells() throws Exception {

        TransactionManager tm = newTransactionManager();

        TTable table = new TTable(hbaseConf, TEST_TABLE);
        HTableInterface htable = table.getHTable();


        // Test shadow cell are created properly
        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put = new Put(row1);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.add(family, qualifier, data1);
        table.put(t2, put);
        tm.commit(t2);

        HBaseTransaction t3 = (HBaseTransaction) tm.begin();
        put = new Put(row3);
        put.add(family, qualifier, data1);
        table.put(t3, put);
        tm.commit(t3);

        // ensure that transaction is no longer in commit table
        // the only place that should have the mapping is the shadow cells
        CommitTable.Client commitTableClient = getTSO().getCommitTable().getClient().get();
        Optional<Long> ct1 = commitTableClient.getCommitTimestamp(t1.getStartTimestamp()).get();
        Optional<Long> ct2 = commitTableClient.getCommitTimestamp(t2.getStartTimestamp()).get();
        Optional<Long> ct3 = commitTableClient.getCommitTimestamp(t3.getStartTimestamp()).get();
        assertFalse("Shouldn't exist in commit table", ct1.isPresent());
        assertFalse("Shouldn't exist in commit table", ct2.isPresent());
        assertFalse("Shouldn't exist in commit table", ct3.isPresent());

        // delete new shadow cell
        Delete del = new Delete(row2);
        del.deleteColumn(family, CellUtils.addShadowCellSuffix(qualifier));
        htable.delete(del);
        htable.flushCommits();

        // verify that we can't read now (since shadow cell is missing)
        Transaction t4 = tm.begin();
        Get get = new Get(row2);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t4, get);
        assertTrue("Should have nothing", getResult.isEmpty());

        Transaction t5 = tm.begin();
        Scan s = new Scan();
        ResultScanner scanner = table.getScanner(t5, s);
        Result result1 = scanner.next();
        Result result2 = scanner.next();
        Result result3 = scanner.next();
        scanner.close();

        assertNull(result3);
        assertTrue("Should have first row", Arrays.equals(result1.getRow(), row1));
        assertTrue("Should have third row", Arrays.equals(result2.getRow(), row3));
        assertTrue("Should have column family", result1.containsColumn(family, qualifier));
        assertTrue("Should have column family", result2.containsColumn(family, qualifier));

        // now add in the previous legacy shadow cell for that row
        put = new Put(row2);
        put.add(family,
                addLegacyShadowCellSuffix(qualifier),
                t2.getStartTimestamp(),
                Bytes.toBytes(t2.getCommitTimestamp()));
        htable.put(put);

        // we should NOT be able to read that row now, even though
        // it has a legacy shadow cell
        Transaction t6 = tm.begin();
        get = new Get(row2);
        get.addColumn(family, qualifier);

        getResult = table.get(t6, get);
        assertFalse("Should NOT have column", getResult.containsColumn(family, qualifier));

        Transaction t7 = tm.begin();
        s = new Scan();
        scanner = table.getScanner(t7, s);
        result1 = scanner.next();
        result2 = scanner.next();
        result3 = scanner.next();
        scanner.close();

        assertNull("There should only be 2 rows", result3);
        assertTrue("Should have first row", Arrays.equals(result1.getRow(), row1));
        assertTrue("Should have third row", Arrays.equals(result2.getRow(), row3));
        assertTrue("Should have column family", result1.containsColumn(family, qualifier));
        assertTrue("Should have column family", result2.containsColumn(family, qualifier));
    }

    /**
     * Test that we cannot use reserved names
     */
    @Test
    public void testReservedNames() throws Exception {
        byte[] nonValidQualifier1 = "blahblah\u0080".getBytes(Charsets.UTF_8);
        byte[] validQualifierIncludingOldShadowCellSuffix = "blahblah:OMID_CTS".getBytes(Charsets.UTF_8);
        TransactionManager tm = newTransactionManager();

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();
        Put put = new Put(row1);
        put.add(family, nonValidQualifier1, data1);
        try {
            table.put(t1, put);
            fail("Shouldn't be able to put this");
        } catch (IllegalArgumentException iae) {
            // correct
        }
        Delete del = new Delete(row1);
        del.deleteColumn(family, nonValidQualifier1);
        try {
            table.delete(t1, del);
            fail("Shouldn't be able to delete this");
        } catch (IllegalArgumentException iae) {
            // correct
        }

        // TODO: Probably we should remove the following test assertions in
        // other commit and move this test into TestShadowCells class
        put = new Put(row1);
        put.add(family, validQualifierIncludingOldShadowCellSuffix, data1);
        try {
            table.put(t1, put);
        } catch (IllegalArgumentException iae) {
            fail("Qualifier shouldn't be rejected anymore");
        }
        del = new Delete(row1);
        del.deleteColumn(family, validQualifierIncludingOldShadowCellSuffix);
        try {
            table.delete(t1, del);
        } catch (IllegalArgumentException iae) {
            fail("Qualifier shouldn't be rejected anymore");
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // Helper methods
    // ////////////////////////////////////////////////////////////////////////

    private static final byte[] LEGACY_SHADOW_CELL_SUFFIX = ":OMID_CTS".getBytes(Charsets.UTF_8);

    private static byte[] addLegacyShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, LEGACY_SHADOW_CELL_SUFFIX);
    }

}
