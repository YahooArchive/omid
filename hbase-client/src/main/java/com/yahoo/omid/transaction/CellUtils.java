package com.yahoo.omid.transaction;

import static com.yahoo.omid.transaction.HBaseTransactionManager.SHADOW_CELL_SUFFIX;
import static com.yahoo.omid.transaction.HBaseTransactionManager.LEGACY_SHADOW_CELL_SUFFIX;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.hash.Hashing;

public class CellUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CellUtils.class);

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
     * Check that the cell passed meets the requirements for a valid cell
     * identifier with Omid. Basically, users can't:
     * 1) specify a timestamp
     * 2) use a particular suffix in the qualifier
     * @param cell
     * @param startTimestamp
     */
    public static void validateCell(Cell cell, long startTimestamp) {
        // Throw exception if timestamp is set by the user
        if (cell.getTimestamp() != HConstants.LATEST_TIMESTAMP
                && cell.getTimestamp() != startTimestamp) {
            throw new IllegalArgumentException(
                    "Timestamp not allowed in transactional user operations");
        }
        // Throw exception if using a non-allowed qualifier
        if (isShadowCell(cell)) {
            throw new IllegalArgumentException(
                    "Reserved string used in column qualifier");
        }
    }

    /**
     * Returns whether a cell contains a qualifier that is a shadow cell
     * column qualifier or not.
     * @param cell
     *            the cell to check if contains the shadow cell qualifier
     * @return whether the cell passed contains a shadow cell qualifier or not
     */
    public static boolean isShadowCell(Cell cell) {
        byte[] qualifier = cell.getQualifierArray();
        int qualOffset = cell.getQualifierOffset();
        int qualLength = cell.getQualifierLength();

        return endsWith(qualifier, qualOffset, qualLength, SHADOW_CELL_SUFFIX)
               ||
               endsWith(qualifier, qualOffset, qualLength, LEGACY_SHADOW_CELL_SUFFIX);
    }

    private static boolean endsWith(byte[] value, int offset, int length, byte[] suffix) {
        if (length <= suffix.length) {
            return false;
        }

        int suffixOffset = offset + length - suffix.length;
        int result = Bytes.compareTo(value, suffixOffset, suffix.length,
                                     suffix, 0, suffix.length);
        return result == 0 ? true : false;
    }

    /**
     * Returns if a cell is marked as a tombstone.
     * @param cell
     *            the cell to check
     * @return whether the cell is marked as a tombstone or not
     */
    public static boolean isTombstone(Cell cell) {
        return CellUtil.matchingValue(cell, TTable.DELETE_TOMBSTONE);
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
                cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                shadowCellQualifier, 0, shadowCellQualifier.length,
                cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                shadowCellValue, 0, shadowCellValue.length);
    }

    /**
     * Analyzes a list of cells, associating the corresponding shadow cell if present.
     *
     * @param cells
     *            the list of cells to classify
     * @return a sorted map associating each cell with its shadow cell
     */
    public static SortedMap<Cell, Optional<Cell>> mapCellsToShadowCells(List<Cell> cells)
            throws IOException {

        SortedMap<Cell, Optional<Cell>> cellToShadowCellMap
            = new TreeMap<Cell, Optional<Cell>>(new CellComparator());

        Map<CellId, Cell> cellIdToCellMap = new HashMap<CellId, Cell>();
        for (Cell cell : cells) {
            if (!isShadowCell(cell)) {
                CellId key = new CellId(CellUtil.cloneRow(cell),
                                        CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        cell.getTimestamp());
                if (cellIdToCellMap.containsKey(key)
                    && !cellIdToCellMap.get(key).equals(cell)) {
                    throw new IOException(
                            "A value is already present for key " + key +
                                    ". This should not happen. Current row elements: " + cells);
                }
                LOG.trace("Adding KV key {} to map with absent value", cell);
                cellToShadowCellMap.put(cell, Optional.<Cell> absent());
                cellIdToCellMap.put(key, cell);
            } else {
                byte[] originalQualifier = removeShadowCellSuffix(CellUtil.cloneQualifier(cell));
                CellId key = new CellId(CellUtil.cloneRow(cell),
                                        CellUtil.cloneFamily(cell),
                                        originalQualifier,
                                        cell.getTimestamp());
                if (cellIdToCellMap.containsKey(key)) {
                    Cell originalCell = cellIdToCellMap.get(key);
                    LOG.trace("Adding to key {} value {}", key, cell);
                    cellToShadowCellMap.put(originalCell, Optional.of(cell));
                } else {
                    LOG.trace("Map does not contain key {}", key);
                }
            }
        }

        return cellToShadowCellMap;
    }

    private static class CellId {

        private static final int MIN_BITS = 32;

        private final byte[] row;
        private final byte[] family;
        private final byte[] qualifier;
        private final long timestamp;

        public CellId(
                byte[] row, byte[] family, byte[] qualifier, long timestamp) {

            this.row = row;
            this.family = family;
            this.qualifier = qualifier;
            this.timestamp = timestamp;
        }

        byte[] getRow() {
            return row;
        }

        byte[] getFamily() {
            return family;
        }

        byte[] getQualifier() {
            return qualifier;
        }

        long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof CellId))
                return false;
            CellId otherCellId = (CellId) o;
            return Arrays.equals(otherCellId.getRow(), row)
                    && Arrays.equals(otherCellId.getFamily(), family)
                    && Arrays.equals(otherCellId.getQualifier(), qualifier)
                    && otherCellId.getTimestamp() == timestamp;
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(MIN_BITS).newHasher()
                    .putBytes(row)
                    .putBytes(family)
                    .putBytes(qualifier)
                    .putLong(timestamp)
                    .hash().asInt();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                          .add("row", Bytes.toStringBinary(row))
                          .add("family", Bytes.toString(family))
                          .add("qualifier", Bytes.toString(qualifier))
                          .add("ts", timestamp)
                          .toString();
        }
    }

    static class CellInfo {

        private final Cell cell;
        private final Cell shadowCell;
        private final long timestamp;

        CellInfo(Cell cell, Cell shadowCell) {
            assert (cell != null && shadowCell != null);
            assert(cell.getTimestamp() == shadowCell.getTimestamp());
            this.cell = cell;
            this.shadowCell = shadowCell;
            this.timestamp = cell.getTimestamp();
        }

        Cell getCell() {
            return cell;
        }

        Cell getShadowCell() {
            return shadowCell;
        }

        long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                          .add("ts", timestamp)
                          .add("cell", cell)
                          .add("shadow cell", shadowCell)
                          .toString();
        }

    }

}
