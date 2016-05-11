/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.transaction;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.testng.Assert.assertEquals;

@Test(groups = "noHBase")
public class TestColumnIterator {

    final byte[] row = Bytes.toBytes("row");
    private final byte[] family1 = Bytes.toBytes("f1");
    private final byte[] family2 = Bytes.toBytes("f2");
    private final byte[] qualifier1 = Bytes.toBytes("c1");
    private final byte[] qualifier2 = Bytes.toBytes("c2");
    final byte[] data = Bytes.toBytes("data");

    private final List<Cell> cells = new ArrayList<Cell>(
            Arrays.asList(
                    // Group 1 (3 elems but grouping should filter shadow cell, so check for 2)
                    new KeyValue(row, family1, qualifier1, 0, data),
                    new KeyValue(row, family1, qualifier1, 1, data),
                    new KeyValue(row, family1, CellUtils.addShadowCellSuffix(qualifier1), 0, data),
                    // Group 2 (2 elems but grouping should filter shadow cell, so check for 1)
                    new KeyValue(row, family1, qualifier2, 0, data),
                    new KeyValue(row, family1, CellUtils.addShadowCellSuffix(qualifier2), 0, data),
                    // Group 3 (2 elems but grouping should filter shadow cell, so check for 1)
                    new KeyValue(row, family2, qualifier1, 0, data),
                    new KeyValue(row, family2, CellUtils.addShadowCellSuffix(qualifier1), 0, data)
            )
    );

    @Test(timeOut = 10_000)
    public void testGroupingCellsByColumnFilteringShadowCells() {

        ImmutableList<Collection<Cell>> groupedColumnsWithoutShadowCells =
                TTable.groupCellsByColumnFilteringShadowCells(cells);
        Log.info("Column Groups " + groupedColumnsWithoutShadowCells);
        assertEquals(groupedColumnsWithoutShadowCells.size(), 3, "Should be 3 column groups");
        int group1Counter = 0;
        int group2Counter = 0;
        int group3Counter = 0;
        for (Collection<Cell> columns : groupedColumnsWithoutShadowCells) {
            for (Cell cell : columns) {
                byte[] cellFamily = CellUtil.cloneFamily(cell);
                byte[] cellQualifier = CellUtil.cloneQualifier(cell);
                // Group 1
                if (Bytes.equals(cellFamily, family1) &&
                        Bytes.equals(cellQualifier, qualifier1)) {
                    group1Counter++;
                }
                // Group 2
                if (Bytes.equals(cellFamily, family1) &&
                        Bytes.equals(cellQualifier, qualifier2)) {
                    group2Counter++;
                }
                // Group 3
                if (Bytes.equals(cellFamily, family2) &&
                        Bytes.equals(cellQualifier, qualifier1)) {
                    group3Counter++;
                }
            }
        }

        assertEquals(group1Counter, 2, "Group 1 should have 2 elems");
        assertEquals(group2Counter, 1, "Group 2 should have 1 elems");
        assertEquals(group3Counter, 1, "Group 3 should have 1 elems");
    }
}
