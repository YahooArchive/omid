package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.LEGACY_SHADOW_CELL_SUFFIX;
import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class TestCellUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestCellUtils.class);

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

    @Test
    public void testCorrectMapingOfCellsToShadowCells() throws IOException {
        // Create the required data
        final byte[] validShadowCellQualifier =
                com.google.common.primitives.Bytes.concat(qualifier, SHADOW_CELL_SUFFIX);

        final byte[] qualifier2 = Bytes.toBytes("test-qual2");
        final byte[] validShadowCellQualifier2 =
                com.google.common.primitives.Bytes.concat(qualifier2, SHADOW_CELL_SUFFIX);

        final byte[] qualifier3 = Bytes.toBytes("test-qual3");

        Cell cell1 = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value")); // Default type is Put
        Cell dupCell1 = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("value")); // Default type is Put
        Cell dupCell1WithAnotherValue = new KeyValue(row, family, qualifier, 1, Bytes.toBytes("other-value"));
        Cell delCell1 = new KeyValue(row, family, qualifier, 1, Type.Delete, Bytes.toBytes("value"));
        Cell shadowCell1 = new KeyValue(row, family, validShadowCellQualifier, 1, Bytes.toBytes("sc-value"));

        Cell cell2 = new KeyValue(row, family, qualifier2, 1, Bytes.toBytes("value2"));
        Cell shadowCell2 = new KeyValue(row, family, validShadowCellQualifier2, 1, Bytes.toBytes("sc-value2"));

        Cell cell3 = new KeyValue(row, family, qualifier3, 1, Bytes.toBytes("value3"));

        // Check a list of cells with duplicate values
        List<Cell> badListWithDups = new ArrayList<>();
        badListWithDups.add(cell1);
        badListWithDups.add(dupCell1WithAnotherValue);
        // Check an exception is thrown when the list contains the same CellId with different values
        try {
            CellUtils.mapCellsToShadowCells(badListWithDups);
        } catch (IOException e) {
            // Expected
        }

        // Check a list of cells with duplicate values
        List<Cell> cellListWithDups = new ArrayList<>();
        cellListWithDups.add(cell1);
        cellListWithDups.add(shadowCell1);
        cellListWithDups.add(dupCell1); // Dup cell
        cellListWithDups.add(delCell1); // Another Dup cell but with different type
        cellListWithDups.add(cell2);
        cellListWithDups.add(cell3);
        cellListWithDups.add(shadowCell2);

        SortedMap<Cell, Optional<Cell>> cellsToShadowCells = CellUtils.mapCellsToShadowCells(cellListWithDups);
        assertEquals("There should be only 3 key-value maps", 3, cellsToShadowCells.size());
        assertTrue(cellsToShadowCells.get(cell1).get().equals(shadowCell1));
        assertTrue(cellsToShadowCells.get(dupCell1).get().equals(shadowCell1));
        assertFalse(cellsToShadowCells.containsKey(delCell1)); // TODO This is strange and needs to be solved.
                                                               // The current algo avoids to put the delete cell
                                                               // as key after the put cell with same value was added
        assertTrue(cellsToShadowCells.get(cell2).get().equals(shadowCell2));
        assertTrue(cellsToShadowCells.get(cell3).equals(Optional.absent()));

    }
}
