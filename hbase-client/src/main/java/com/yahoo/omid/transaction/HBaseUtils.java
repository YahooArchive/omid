package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {

    /**
     * Returns true if the particular cell passed exists in the datastore.
     * 
     * @param table
     * @param row
     * @param family
     * @param qualifier
     * @param version
     * @return true if the cell specified exists. false otherwise
     * @throws IOException
     */
    public static boolean hasCell(TTable table, byte[] row, byte[] family, byte[] qualifier, long version)
            throws IOException {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setTimeStamp(version);

        Result result = table.getHTable().get(get);
        return result.containsColumn(family, qualifier);
    }

    /**
     * Returns true if the key value passed has a corresponding shadow cell
     * in the datastore.
     * 
     * @param kv
     * @param table
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(HTableInterface table, KeyValue kv)
            throws IOException {
        return hasShadowCell(table, kv.getRow(), kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell
     * in the datastore.
     * 
     * @param table
     * @param row
     * @param family
     * @param qualifier
     * @param version
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(TTable table, byte[] row, byte[] family, byte[] qualifier, long version)
            throws IOException {
        return hasShadowCell(table.getHTable(), row, family, qualifier, version);
    }

    /**
     * Returns true if the particular cell passed has a corresponding shadow cell
     * in the datastore.
     * 
     * @param table
     * @param row
     * @param family
     * @param qualifier
     * @param version
     * @return true if it has a shadow cell. false otherwise.
     * @throws IOException
     */
    public static boolean hasShadowCell(HTableInterface table, byte[] row, byte[] family, byte[] qualifier, long version)
            throws IOException {
        Get get = new Get(row);
        byte[] sc = addShadowCellSuffix(qualifier);
        get.addColumn(family, sc);
        get.setTimeStamp(version);

        Result result = table.get(get);
        return result.containsColumn(family, sc);
    }

    /**
     * Adds the shadow cell suffix to an HBase column qualifier.
     * 
     * @param qualifier
     *            the qualifier to add the suffix to.
     * @return the suffixed qualifier
     */
    public static byte[] addShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, SHADOW_CELL_SUFFIX);
    }

    /**
     * Removes the shadow cell suffix from an HBase column qualifier.
     * 
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
     * 
     * @param qualifier
     *            the qualifier to learn whether is a shadow cell or not.
     * @return whether the qualifier passed is a shadow cell or not
     */
    public static boolean isShadowCell(byte[] qualifier) {
        int index = com.google.common.primitives.Bytes.indexOf(qualifier, SHADOW_CELL_SUFFIX);
        return index >= 0 && index == (qualifier.length - SHADOW_CELL_SUFFIX.length);
    }

}