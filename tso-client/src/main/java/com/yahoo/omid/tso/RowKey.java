/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.google.common.base.Charsets.UTF_8;
import com.google.common.hash.Hashing;

public class RowKey {
    private byte[] rowId;
    private byte[] tableId;
    private int hash = 0;

    public RowKey() {
        rowId = new byte[0];
        tableId = new byte[0];
    }

    public RowKey(byte[] r, byte[] t) {
        rowId = r;
        tableId = t;
    }

    public byte[] getTable() {
        return tableId;
    }

    public byte[] getRow() {
        return rowId;
    }

    public String toString() {
        return new String(tableId, UTF_8) + ":" + new String(rowId, UTF_8);
    }

    public boolean equals(Object obj) {
        if (obj instanceof RowKey) {
            RowKey other = (RowKey) obj;

            return Arrays.equals(other.rowId, rowId)
                    && Arrays.equals(other.tableId, tableId);
        }
        return false;
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        hash = Hashing.murmur3_32().newHasher()
            .putBytes(tableId).putBytes(rowId)
            .hash().asInt();
        return hash;
    }
}
