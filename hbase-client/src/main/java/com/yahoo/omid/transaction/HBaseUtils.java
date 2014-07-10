package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

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
        return hasCell(row,
                       family,
                       addShadowCellSuffix(qualifier),
                       version,
                       cellGetter);
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell in the datastore.
     * @param cell
     * @param cellGetter
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(Cell cell, CellGetter cellGetter) throws IOException {
        return hasCell(CellUtil.cloneRow(cell),
                       CellUtil.cloneFamily(cell),
                       addShadowCellSuffix(CellUtil.cloneQualifier(cell)),
                       cell.getTimestamp(),
                       cellGetter);
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
        if (shadowCellSuffixIdx < 0) {
            throw new IOException("Can not get qualifier from shadow cell");
        }
        return Arrays.copyOfRange(qualifier, 0, shadowCellSuffixIdx);
    }

    /**
     * Returns if a qualifier is a shadow cell column qualifier.
     * @param qualifier
     *            the qualifier to learn whether is a shadow cell or not.
     * @return whether the qualifier passed is a shadow cell or not
     */
    public static boolean isShadowCell(byte[] qualifier) {
        int index = com.google.common.primitives.Bytes.indexOf(qualifier, SHADOW_CELL_SUFFIX);
        return index >= 0 && index == (qualifier.length - SHADOW_CELL_SUFFIX.length);
    }

}