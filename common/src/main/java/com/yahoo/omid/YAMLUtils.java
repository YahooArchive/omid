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

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class YAMLUtils<T> {

    private static final Logger LOG = LoggerFactory.getLogger(YAMLUtils.class);

    private Joiner joiner = Joiner.on(", ");

    public void loadDefaultSettings(List<String> fileNames, Object bean) {

        try {
            BeanUtils.populate(bean, new YAMLUtils<Map>().loadDefaultSettings(fileNames));
        } catch (IllegalAccessException | InvocationTargetException | IOException e) {
            throw new IllegalStateException(e);
        }

    }

    public T loadDefaultSettings(List<String> fileNames) throws IOException {

        String content = loadResourceWithFallback(fileNames);
        LOG.debug("Loaded config file:\n{}", content);
        return (T) new Yaml().load(content);

    }

    private String loadResourceWithFallback(List<String> fileNames) throws IOException {

        for (String fileName : fileNames) {
            try {
                return Resources.toString(Resources.getResource(fileName), Charset.forName("UTF-8"));
            } catch (IllegalArgumentException e) {
                LOG.debug("Resource {} not found, fallback to default", fileName);
            }
        }
        throw new IOException(String.format("Not a single resource found: %s", joiner.join(fileNames)));

    }

}
