package com.yahoo.omid.transaction;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tm.Transaction;
import com.yahoo.omid.tm.TransactionManager;

public class TestDeletion extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestDeletion.class);

    byte[] famA = Bytes.toBytes(TEST_FAMILY);
    byte[] famB = Bytes.toBytes(TEST_FAMILY2);
    byte[] colA = Bytes.toBytes("testdataA");
    byte[] colB = Bytes.toBytes("testdataB");
    byte[] data1 = Bytes.toBytes("testWrite-1");
    byte[] data2 = Bytes.toBytes("testWrite-2");
    byte[] modrow = Bytes.toBytes("test-del" + 3);

    static class FamCol {
        final byte[] fam;
        final byte[] col;

        public FamCol(byte[] fam, byte[] col) {
            this.fam = fam;
            this.col = col;
        }

    }

    @Test
    public void runTestDeleteFamily() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;
        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famB, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteFamily(famA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    @Test
    public void runTestDeleteColumn() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteColumn(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);
        assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    /**
     * This test is very similar to #runTestDeleteColumn() but exercises Delete#deleteColumns()
     */
    @Test
    public void runTestDeleteColumns() throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        FamCol famColB = new FamCol(famA, colB);
        writeRows(tt, t1, rowsWritten, famColA, famColB);
        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        d.deleteColumns(famA, colA);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        Map<FamCol, Integer> count = countColsInRows(rs, famColA, famColB);
        assertEquals("ColA count should be equal to rowsWritten", rowsWritten, (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        count = countColsInRows(rs, famColA, famColB);

        assertEquals("ColA count should be equal to rowsWritten - 1", (rowsWritten - 1), (int) count.get(famColA));
        assertEquals("ColB count should be equal to rowsWritten", rowsWritten, (int) count.get(famColB));
    }

    @Test
    public void runTestDeleteRow() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        int rowsWritten = 10;

        FamCol famColA = new FamCol(famA, colA);
        writeRows(tt, t1, rowsWritten, famColA);

        tm.commit(t1);

        Transaction t2 = tm.begin();
        Delete d = new Delete(modrow);
        tt.delete(t2, d);

        Transaction tscan = tm.begin();
        ResultScanner rs = tt.getScanner(tscan, new Scan());

        int rowsRead = countRows(rs);
        assertTrue("Expected " + rowsWritten + " rows but " + rowsRead + " found",
                rowsRead == rowsWritten);

        tm.commit(t2);

        tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan());

        rowsRead = countRows(rs);
        assertTrue("Expected " + (rowsWritten - 1) + " rows but " + rowsRead + " found",
                rowsRead == (rowsWritten - 1));

    }

    private int countRows(ResultScanner rs) throws IOException {
        int count;
        Result r = rs.next();
        count = 0;
        while (r != null) {
            count++;
            LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
            r = rs.next();
        }
        return count;
    }

    private void writeRows(TTable tt, Transaction t1, int rowcount, FamCol... famCols) throws IOException {
        for (int i = 0; i < rowcount; i++) {
            byte[] row = Bytes.toBytes("test-del" + i);

            Put p = new Put(row);
            for (FamCol col : famCols) {
                p.add(col.fam, col.col, data1);
            }
            tt.put(t1, p);
        }
    }

    private Map<FamCol, Integer> countColsInRows(ResultScanner rs, FamCol... famCols) throws IOException {
        Map<FamCol, Integer> colCount = new HashMap<FamCol, Integer>();
        Result r = rs.next();
        while (r != null) {
            for (FamCol col : famCols) {
                if (r.containsColumn(col.fam, col.col)) {
                    Integer c = colCount.get(col);

                    if (c == null) {
                        colCount.put(col, 1);
                    } else {
                        colCount.put(col, c + 1);
                    }
                }
            }
            r = rs.next();
        }
        return colCount;
    }

}
