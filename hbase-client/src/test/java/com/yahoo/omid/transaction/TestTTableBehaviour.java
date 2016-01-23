package com.yahoo.omid.transaction;

import com.google.common.base.Charsets;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

@Test(groups = "noHBase")
public class TestTTableBehaviour {

    private byte[] row = Bytes.toBytes("1row");
    private byte[] famName = Bytes.toBytes("tf");
    private byte[] colName = Bytes.toBytes("tc");
    private byte[] dataValue = Bytes.toBytes("test-data");

    @Test(timeOut = 10_000)
    public void testUserOperationsDontAllowTimestampSpecification() throws Exception {

        // Component under test
        TTable tt = new TTable(Mockito.mock(HTableInterface.class), Mockito.mock(HTableInterface.class));

        long randomTimestampValue = Bytes.toLong("deadbeef".getBytes());

        Transaction tx = Mockito.mock(Transaction.class);

        // Test put fails when a timestamp is specified in the put
        Put put = new Put(row, randomTimestampValue);
        put.add(famName, colName, dataValue);
        try {
            tt.put(tx, put);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test put fails when a timestamp is specified in a qualifier
        put = new Put(row);
        put.add(famName, colName, randomTimestampValue, dataValue);
        try {
            tt.put(tx, put);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test that get fails when a timestamp is specified
        Get get = new Get(row);
        get.setTimeStamp(randomTimestampValue);
        try {
            tt.get(tx, get);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test scan fails when a timerange is specified
        Scan scan = new Scan(get);
        try {
            tt.getScanner(tx, scan);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test delete fails when a timestamp is specified
        Delete delete = new Delete(row);
        delete.setTimestamp(randomTimestampValue);
        try {
            tt.delete(tx, delete);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test delete fails when a timestamp is specified in a qualifier
        delete = new Delete(row);
        delete.deleteColumn(famName, colName, randomTimestampValue);
        try {
            tt.delete(tx, delete);
            fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

    }

    /**
     * Test that we cannot use reserved names for shadow cell identifiers as qualifiers in user operations
     */
    @Test(timeOut = 10_000)
    public void testReservedNamesForShadowCellsCanNotBeUsedAsQualifiersInUserOperations() throws Exception {
        byte[] nonValidQualifier1 = "blahblah\u0080".getBytes(Charsets.UTF_8);
        byte[] validQualifierIncludingOldShadowCellSuffix = "blahblah:OMID_CTS".getBytes(Charsets.UTF_8);

        TTable table = new TTable(Mockito.mock(HTableInterface.class), Mockito.mock(HTableInterface.class));

        HBaseTransaction t1 = Mockito.mock(HBaseTransaction.class);
        Put put = new Put(row);
        put.add(famName, nonValidQualifier1, dataValue);
        try {
            table.put(t1, put);
            fail("Shouldn't be able to put this");
        } catch (IllegalArgumentException iae) {
            // correct
        }
        Delete del = new Delete(row);
        del.deleteColumn(famName, nonValidQualifier1);
        try {
            table.delete(t1, del);
            fail("Shouldn't be able to delete this");
        } catch (IllegalArgumentException iae) {
            // correct
        }

        put = new Put(row);
        put.add(famName, validQualifierIncludingOldShadowCellSuffix, dataValue);
        try {
            table.put(t1, put);
        } catch (IllegalArgumentException iae) {
            fail("Qualifier shouldn't be rejected anymore");
        }
        del = new Delete(row);
        del.deleteColumn(famName, validQualifierIncludingOldShadowCellSuffix);
        try {
            table.delete(t1, del);
        } catch (IllegalArgumentException iae) {
            fail("Qualifier shouldn't be rejected anymore");
        }
    }

}
