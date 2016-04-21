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
package org.apache.omid.tso;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.omid.HBaseConfigModule;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.omid.tso.LeaseManagement.LeaseManagementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

import static org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig.DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME;

public class VoidLeaseManagementModule extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(VoidLeaseManagementModule.class);
    private String keytab;
    private String principal;
    private String tableName = DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME;

    @Override
    protected void configure() {
        LOG.info(this.toString());
        bindConstant().annotatedWith(Names.named(HBaseTimestampStorageConfig.TIMESTAMP_STORAGE_TABLE_NAME_KEY)).to(tableName);
        install(new HBaseConfigModule(principal, keytab));
    }

    @Provides
    @Singleton
    LeaseManagement provideLeaseManager(TSOChannelHandler tsoChannelHandler, TSOStateManager stateManager)
            throws LeaseManagementException {

        return new VoidLeaseManager(tsoChannelHandler, stateManager);

    }

    // ----------------------------------------------------------------------------------------------------------------
    // WARNING: Do not remove getters/setters, needed by snake_yaml!
    // ----------------------------------------------------------------------------------------------------------------

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VoidLeaseManagementModule{");
        sb.append("keytab='").append(keytab).append('\'');
        sb.append(", principal='").append(principal).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
