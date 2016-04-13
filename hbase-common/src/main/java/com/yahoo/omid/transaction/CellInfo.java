/**
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
package com.yahoo.omid.transaction;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.Cell;

public class CellInfo {

    private final Cell cell;
    private final Cell shadowCell;
    private final long timestamp;

    public CellInfo(Cell cell, Cell shadowCell) {
        // TODO: Use Guava preconditions instead of the assertions
        assert (cell != null && shadowCell != null);
        assert (cell.getTimestamp() == shadowCell.getTimestamp());
        this.cell = cell;
        this.shadowCell = shadowCell;
        this.timestamp = cell.getTimestamp();
    }

    public Cell getCell() {
        return cell;
    }

    public Cell getShadowCell() {
        return shadowCell;
    }

    public long getTimestamp() {
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
