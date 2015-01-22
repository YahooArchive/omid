package com.yahoo.omid.transaction;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class TestHBaseUtils {

    private final String QUALIFIER = "col";

    private ByteArrayOutputStream baos;

    @BeforeMethod
    public void beforeTest() {
        baos = new ByteArrayOutputStream();
    }

    @AfterMethod
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
                CellUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier if there's nothing else apart from the suffix
        baos.write(SHADOW_CELL_SUFFIX);
        assertFalse("Should not be a valid shadowCell identifier",
                CellUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier because the suffix is not at the end
        // of the qualifier
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(Bytes.toBytes(QUALIFIER));
        assertFalse("Should not be a valid shadowCell identifier",
                CellUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

        // Test the qualifier passed is not a shadow cell
        // qualifier because the suffix appears more than once
        baos.write(Bytes.toBytes(QUALIFIER));
        baos.write(SHADOW_CELL_SUFFIX);
        baos.write(SHADOW_CELL_SUFFIX);
        assertFalse("Should not be a valid shadowCell identifier",
                CellUtils.isShadowCell(baos.toByteArray()));
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
                CellUtils.isShadowCell(baos.toByteArray()));
        baos.reset();

    }

}
