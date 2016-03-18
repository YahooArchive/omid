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
package com.yahoo.omid;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.tools.hbase.SecureHBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class HBaseConfigModule extends AbstractModule {
    private String principal;
    private String keytab;

    public HBaseConfigModule(String principal, String keytab) {
        this.principal = principal;
        this.keytab = keytab;
    }

    @Override
    protected void configure() {
    }

    @Provides
    public Configuration provideHBaseConfig() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        SecureHBaseConfig secureHBaseConfig = new SecureHBaseConfig();
        secureHBaseConfig.setKeytab(keytab);
        secureHBaseConfig.setPrincipal(principal);
        HBaseLogin.loginIfNeeded(secureHBaseConfig);
        return configuration;

    }

    // Allow this module to be installed multiple times


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HBaseConfigModule that = (HBaseConfigModule) o;

        if (principal != null ? !principal.equals(that.principal) : that.principal != null) return false;
        return keytab != null ? keytab.equals(that.keytab) : that.keytab == null;

    }

    @Override
    public int hashCode() {
        int result = principal != null ? principal.hashCode() : 0;
        result = 31 * result + (keytab != null ? keytab.hashCode() : 0);
        return result;
    }

}
