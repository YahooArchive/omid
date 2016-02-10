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
package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Constants related to Commit Table
 */
public class CommitTableConstants {

    public static final String COMMIT_TABLE_NAME_KEY = "omid.committable.tablename";
    public static final String COMMIT_TABLE_DEFAULT_NAME = "OMID_COMMIT_TABLE";
    public static final byte[] COMMIT_TABLE_FAMILY = "F".getBytes(UTF_8);

    public static final byte[] LOW_WATERMARK_FAMILY = "LWF".getBytes(UTF_8);

    // Avoid instantiation
    private CommitTableConstants() {
    }

}
