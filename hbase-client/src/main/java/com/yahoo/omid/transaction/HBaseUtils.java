package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;
import static com.yahoo.omid.transaction.HBaseTransactionManager.LEGACY_SHADOW_CELL_SUFFIX;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

    /**
     * Utility interface to get rid of the dependency on HBase server package
     */
    public interface CellGetter {
        public Result get(Get get) throws IOException;
    }

    /**
     * Returns true if the particular cell passed exists in the datastore.
     * @param row
     * @param family
     * @param qualifier
     * @param version
     * @param cellGetter
     * @return true if the cell specified exists. false otherwise
     * @throws IOException
     */
    public static boolean hasCell(byte[] row,
                                  byte[] family,
                                  byte[] qualifier,
                                  long version,
                                  CellGetter cellGetter)
    throws IOException {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setTimeStamp(version);

        Result result = cellGetter.get(get);

        return result.containsColumn(family, qualifier);
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell in the datastore.
     * @param row
     * @param family
     * @param qualifier
     * @param version
     * @param cellGetter
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(byte[] row,
                                        byte[] family,
                                        byte[] qualifier,
                                        long version,
                                        CellGetter cellGetter) throws IOException {
        return hasCell(row, family, addShadowCellSuffix(qualifier),
                       version, cellGetter)
            || hasCell(row, family, addLegacyShadowCellSuffix(qualifier),
                       version, cellGetter);
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell in the datastore.
     * @param cell
     * @param cellGetter
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(Cell cell, CellGetter cellGetter) throws IOException {
        return hasCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
                       addShadowCellSuffix(CellUtil.cloneQualifier(cell)),
                       cell.getTimestamp(), cellGetter)
            || hasCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
                       addLegacyShadowCellSuffix(CellUtil.cloneQualifier(cell)),
                       cell.getTimestamp(), cellGetter);
    }

    /**
     * Adds the shadow cell suffix to an HBase column qualifier.
     * @param qualifier
     *            the qualifier to add the suffix to.
     * @return the suffixed qualifier
     */
    public static byte[] addShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, SHADOW_CELL_SUFFIX);
    }

    public static byte[] addLegacyShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, LEGACY_SHADOW_CELL_SUFFIX);
    }

    /**
     * Removes the shadow cell suffix from an HBase column qualifier.
     * @param qualifier
     *            the qualifier to remove the suffix from.
     * @return the qualifier without the suffix
     * @throws IOException
     *             when there's no suffix to remove
     */
    public static byte[] removeShadowCellSuffix(byte[] qualifier) throws IOException {

        int shadowCellSuffixIdx = com.google.common.primitives.Bytes.indexOf(qualifier,
                HBaseTransactionManager.SHADOW_CELL_SUFFIX);
        int shadowCellSuffixIdx2 = com.google.common.primitives.Bytes.indexOf(qualifier,
                HBaseTransactionManager.LEGACY_SHADOW_CELL_SUFFIX);
        if (shadowCellSuffixIdx >= 0) {
            return Arrays.copyOfRange(qualifier, 0, shadowCellSuffixIdx);
        } else if (shadowCellSuffixIdx2 >= 0) {
            return Arrays.copyOfRange(qualifier, 0, shadowCellSuffixIdx2);
        } else {
            throw new IOException("Can not get qualifier from shadow cell");
        }
    }

    /**
     * Returns if a qualifier is a shadow cell column qualifier.
     * @param qualifier
     *            the qualifier to learn whether is a shadow cell or not.
     * @return whether the qualifier passed is a shadow cell or not
     */
    public static boolean isShadowCell(byte[] qualifier) {
        int index = com.google.common.primitives.Bytes.indexOf(qualifier, SHADOW_CELL_SUFFIX);
        int index2 = com.google.common.primitives.Bytes.indexOf(qualifier, LEGACY_SHADOW_CELL_SUFFIX);
        return index > 0 && index == (qualifier.length - SHADOW_CELL_SUFFIX.length)
            || index2 > 0 && index2 == (qualifier.length - LEGACY_SHADOW_CELL_SUFFIX.length);
    }

    /**
     * Returns a new shadow cell created from a particular cell.
     * @param cell
     *            the cell to reconstruct the shadow cell from.
     * @param shadowCellValue
     *            the value for the new shadow cell created
     * @return the brand-new shadow cell
     */
    public static Cell buildShadowCellFromCell(Cell cell, byte[] shadowCellValue) {
        byte[] shadowCellQualifier =
                addShadowCellSuffix(CellUtil.cloneQualifier(cell));cell.getTypeByte();
        return new KeyValue(
                cell.getRowArray(), cell.getRowOffset(), (int) cell.getRowLength(),
                cell.getFamilyArray(), cell.getFamilyOffset(), (int) cell.getFamilyLength(),
                shadowCellQualifier, 0, shadowCellQualifier.length,
                cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                shadowCellValue, 0, shadowCellValue.length);
    }

}
