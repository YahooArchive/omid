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
package org.apache.omid.tools.hbase;

import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.omid.tools.hbase.OmidTableManager.COMMIT_TABLE_COMMAND_NAME;
import static org.apache.omid.tools.hbase.OmidTableManager.TIMESTAMP_TABLE_COMMAND_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOmidTableManager {

    private HBaseTestingUtility hBaseTestUtil;
    private Configuration hbaseConf;
    private HBaseAdmin hBaseAdmin;

    @BeforeClass
    public void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        hBaseTestUtil = new HBaseTestingUtility(hbaseConf);
        hBaseTestUtil.startMiniCluster(1);

        hBaseAdmin = hBaseTestUtil.getHBaseAdmin();
    }

    @AfterClass
    public void tearDownClass() throws Exception {

        hBaseAdmin.close();

        hBaseTestUtil.shutdownMiniCluster();

    }

    @Test(timeOut = 20_000)
    public void testCreateDefaultTimestampTableSucceeds() throws Throwable {

        String[] args = new String[]{TIMESTAMP_TABLE_COMMAND_NAME};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf(HBaseTimestampStorageConfig.DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME);

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 1, "Should have only 1 region");

    }

    @Test(timeOut = 20_000)
    public void testCreateDefaultCommitTableSucceeds() throws Throwable {

        String[] args = new String[]{COMMIT_TABLE_COMMAND_NAME};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf(HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_NAME);

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 16, "Should have 16 regions");

    }

    @Test(timeOut = 20_000)
    public void testCreateCustomCommitTableSucceeds() throws Throwable {

        String[] args = new String[]{COMMIT_TABLE_COMMAND_NAME, "-tableName", "my-commit-table", "-numRegions", "1"};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf("my-commit-table");

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 1, "Should have only 1 regions");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, timeOut = 20_000)
    public void testExceptionIsThrownWhenSpecifyingAWrongCommand() throws Throwable {

        String[] args = new String[]{"non-recognized-command"};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

    }

}
