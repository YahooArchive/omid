/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.transaction;

import com.yahoo.omid.transaction.CellUtils.CellGetter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

public class TTableCellGetterAdapter implements CellGetter {

    private final TTable txTable;

    public TTableCellGetterAdapter(TTable txTable) {
        this.txTable = txTable;
    }

    public Result get(Get get) throws IOException {
        return txTable.getHTable().get(get);
    }

}