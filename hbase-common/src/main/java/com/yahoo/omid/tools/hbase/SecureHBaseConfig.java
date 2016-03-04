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
package com.yahoo.omid.tools.hbase;

import com.beust.jcommander.Parameter;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class SecureHBaseConfig {

    @Parameter(names = "-hbaseClientPrincipal", description = "The kerberos principal for HBase clients")
    private String principal = "not set";

    @Parameter(names = "-hbaseClientKeytab", description = "Path to HBase client keytab")
    private String keytab = "not set";

    public String getPrincipal() {
        return principal;
    }

    public String getKeytab() {
        return keytab;
    }

    @Inject(optional = true)
    @Named("hbase.client.principal")
    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Inject(optional = true)
    @Named("hbase.client.keytab")
    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

}
