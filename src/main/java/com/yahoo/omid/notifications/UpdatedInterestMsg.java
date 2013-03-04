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
package com.yahoo.omid.notifications;

import org.apache.hadoop.hbase.util.Bytes;


public final class UpdatedInterestMsg {
    public final String interest;
    public final byte[] rowKey;
    
    public UpdatedInterestMsg(String interest, byte[] rowKey) {
        this.interest = interest;
        this.rowKey = rowKey;
    }

    @Override
    public String toString() {
        return "UpdatedInterestMsg [interest=" + interest + ", rowKey=" + Bytes.toString(rowKey) + "]";
    }
    
    
}
