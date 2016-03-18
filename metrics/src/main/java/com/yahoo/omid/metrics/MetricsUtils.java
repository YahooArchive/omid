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
package com.yahoo.omid.metrics;

public class MetricsUtils {

    private static final char DEFAULT_SEPARATOR = '.';

    public static String name(String name, String... otherNames) {
        return name(name, DEFAULT_SEPARATOR, otherNames);
    }

    public static String name(String name, char separator, String... otherNames) {
        final StringBuffer builder = new StringBuffer(name);
        if (otherNames != null) {
            for (String otherName : otherNames) {
                concat(builder, otherName, separator);
            }
        }
        return builder.toString();
    }

    private static void concat(StringBuffer head, String tail, char separator) {
        if (tail != null && !tail.isEmpty()) {
            if (head.length() > 0) {
                head.append(separator);
            }
            head.append(tail);
        }
    }

}
