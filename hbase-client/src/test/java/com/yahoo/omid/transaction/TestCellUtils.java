package com.yahoo.omid.transaction;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.yahoo.omid.transaction.HBaseTransactionManager.LEGACY_SHADOW_CELL_SUFFIX;
import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class TestCellUtils {

    private final byte[] row = Bytes.toBytes("test-row");
    private final byte[] family = Bytes.toBytes("test-family");
    private final byte[] qualifier = Bytes.toBytes("test-qual");

    @DataProvider(name = "shadow-cell-suffixes")
    public Object[][] createShadowCellSuffixes() {
        return new Object[][] {
                { SHADOW_CELL_SUFFIX },
                { LEGACY_SHADOW_CELL_SUFFIX },
        };
    }

    @Test(dataProvider = "shadow-cell-suffixes")
    public void testShadowCellQualifiers(byte[] shadowCellSuffixToTest) throws IOException {

        final byte[] isolatedNonValidShadowCellQualifier = shadowCellSuffixToTest;
        final byte[] validShadowCellQualifier =
                com.google.common.primitives.Bytes.concat(qualifier, isolatedNonValidShadowCellQualifier);
        final byte[] sandwichValidShadowCellQualifier =
                com.google.common.primitives.Bytes.concat(shadowCellSuffixToTest, validShadowCellQualifier);
        final byte[] doubleEndedValidShadowCellQualifier =
                com.google.common.primitives.Bytes.concat(validShadowCellQualifier, isolatedNonValidShadowCellQualifier);
        final byte[] interleavedValidShadowCellQualifier =
                com.google.common.primitives.Bytes.concat(validShadowCellQualifier,
                        com.google.common.primitives.Bytes.concat(validShadowCellQualifier, validShadowCellQualifier));
        final byte[] value = Bytes.toBytes("test-value");

        // Test the qualifier passed is a shadow cell
        // qualifier because it contains only one suffix
        // and is placed at the end of the qualifier:
        // qual_nameSUFFIX
        KeyValue kv = new KeyValue(row, family, validShadowCellQualifier, value);
        assertTrue("Should include a valid shadowCell identifier", CellUtils.isShadowCell(kv));

        // We also accept this pattern in the qualifier:
        // SUFFIXqual_nameSUFFIX
        kv = new KeyValue(row, family, sandwichValidShadowCellQualifier, value);
        assertTrue("Should include a valid shadowCell identifier", CellUtils.isShadowCell(kv));

        // We also accept this pattern in the qualifier:
        // qual_nameSUFFIXSUFFIX
        kv = new KeyValue(row, family, doubleEndedValidShadowCellQualifier, value);
        assertTrue("Should include a valid shadowCell identifier", CellUtils.isShadowCell(kv));

        // We also accept this pattern in the qualifier:
        // qual_nameSUFFIXqual_nameSUFFIXqual_nameSUFFIX
        kv = new KeyValue(row, family, interleavedValidShadowCellQualifier, value);
        assertTrue("Should include a valid shadowCell identifier", CellUtils.isShadowCell(kv));

        // Test the qualifier passed is not a shadow cell
        // qualifier if there's nothing else apart from the suffix
        kv = new KeyValue(row, family, isolatedNonValidShadowCellQualifier, value);
        assertFalse("Should not include a valid shadowCell identifier", CellUtils.isShadowCell(kv));

    }

}
