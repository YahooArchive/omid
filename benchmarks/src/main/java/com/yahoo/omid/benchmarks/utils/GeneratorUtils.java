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
package com.yahoo.omid.benchmarks.utils;

import java.util.Random;

public final class GeneratorUtils {

    public static IntegerGenerator getIntGenerator(Distribution distribution) {
        switch (distribution) {
            case UNIFORM:
                return new IntegerGenerator() {
                    Random r = new Random();

                    @Override
                    public int nextInt() {
                        return r.nextInt(Integer.MAX_VALUE);
                    }

                    @Override
                    public double mean() {
                        return 0;
                    }
                };
            case ZIPFIAN:
            default:
                return new ScrambledZipfianGenerator(Long.MAX_VALUE);
        }
    }

}
