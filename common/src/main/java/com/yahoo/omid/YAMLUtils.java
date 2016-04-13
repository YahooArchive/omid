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
package com.yahoo.omid;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class YAMLUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YAMLUtils.class);

    public void loadSettings(String resourcePath, String defaultResourcePath, Object bean) {
        try {
            Map properties = loadSettings(resourcePath, defaultResourcePath);
            BeanUtils.populate(bean, properties);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void loadSettings(String resourcePath, Object bean) {
        try {
            Map properties = loadSettings(null, resourcePath);
            BeanUtils.populate(bean, properties);
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map loadSettings(String resourcePath, String defaultResourcePath) throws IOException {
        Map defaultSetting = loadAsMap(defaultResourcePath);
        Preconditions.checkState(defaultSetting.size() > 0, String.format("Failed to load file '%s' from classpath", defaultResourcePath));
        if (resourcePath != null) {
            Map userSetting = loadAsMap(resourcePath);
            defaultSetting.putAll(userSetting);
        }
        return defaultSetting;
    }

    @SuppressWarnings("unchecked")
    private Map loadAsMap(String path) throws IOException {
        try {
            String content = Resources.toString(Resources.getResource(path), Charset.forName("UTF-8"));
            LOG.debug("Loaded resource file '{}'\n{}", path, content);
            Map settings = new Yaml().loadAs(content, Map.class);
            if (settings == null) {
                settings = new HashMap(0);
            }
            return settings;
        } catch (IllegalArgumentException e) {
            return new HashMap();
        }
    }

}
