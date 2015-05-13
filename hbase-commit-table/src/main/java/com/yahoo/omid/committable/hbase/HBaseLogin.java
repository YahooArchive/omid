/**
 * Copyright 2011-2015 Yahoo Inc.
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

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public final class HBaseLogin {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseLogin.class);

    public static class Config {
        @Parameter(names = "-hbaseClientPrincipal", description = "The kerberos principal for HBase clients")
        private String principal = "omid_hbase_client";

        @Parameter(names = "-hbaseClientKeytab", description = "Path to HBase client keytab")
        private String keytab = "/path/to/hbase/client/keytab";

        public String getPrincipal() {
            return principal;
        }

        public String getKeytab() {
            return keytab;
        }
    }

    public static UserGroupInformation loginIfNeeded(Config config) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.info("Security is enabled, logging in with principal={}, keytab={}",
                    config.getPrincipal(), config.getKeytab());
            UserGroupInformation.loginUserFromKeytab(config.getPrincipal(), config.getKeytab());
        }
        return UserGroupInformation.getCurrentUser();
    }
}
