package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHBaseUtils {

    private final String QUALIFIER = "col";

    private ByteArrayOutputStream baos;

    @Before
    public void beforeTest() {
        baos = new ByteArrayOutputStream();
    }

    @After
    public void afterTest() throws IOException {
        if (baos != null) {
            baos.close();
        }
    }

    @Test
    public void testIsShadowCell() throws IOException {

        // Test the qualifier passed is a shadow cell
        // qualifier because it contains only one suffix
        // andis placed at the end of the qualifier
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        assertTrue("Should be a valid shadowCell",
                HBaseUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier if there's nothing else apart from the suffix
        baos.write(SHADOW_CELL_SUFFIX);
        assertFalse("Should not be a valid shadowCell identifier",
                HBaseUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier because the suffix is not at the end
        // of the qualifier
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(Bytes.toBytes(QUALIFIER));
        assertFalse("Should not be a valid shadowCell identifier",
                HBaseUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier because the suffix appears more than once
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(SHADOW_CELL_SUFFIX);
        assertFalse("Should not be a valid shadowCell identifier",
                HBaseUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier because the suffix appears more than once
        // interleaved
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        assertFalse("Should not be a valid shadowCell identifier",
                HBaseUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

    }

}