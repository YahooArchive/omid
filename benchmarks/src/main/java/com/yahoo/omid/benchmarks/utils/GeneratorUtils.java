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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import com.yahoo.omid.benchmarks.tso.IntegerGenerator;
import com.yahoo.omid.benchmarks.tso.ScrambledZipfianGenerator;

import java.util.Random;

public final class GeneratorUtils {

    public enum RowDistribution {

        UNIFORM, ZIPFIAN;

        public static RowDistribution fromString(String rowDistributionAsString) {

            for (RowDistribution rd : RowDistribution.values()) {
                if (rd.toString().equalsIgnoreCase(rowDistributionAsString)) {
                    return rd;
                }
            }

            return null;
        }

    }

    public static class RowDistributionConverter implements IStringConverter<RowDistribution> {

        @Override
        public RowDistribution convert(String value) {
            RowDistribution rowDistribution = RowDistribution.fromString(value);
            if (null == rowDistribution) {
                throw new ParameterException("Value " + value +
                        " can't be converted to RowDistribution enum." +
                        " Avaliable values are: uniform (default) | zipfian");
            }
            return rowDistribution;
        }

    }

    public static IntegerGenerator getIntGenerator(RowDistribution rowDistribution) {
        switch (rowDistribution) {
            case UNIFORM:
                return new IntegerGenerator() {
                    Random r = new Random();

                    @Override
                    public int nextInt() {
                        return r.nextInt(Integer.MAX_VALUE);
                    }

                    @Override
                    public double mean() {
                        // TODO Auto-generated method stub
                        return 0;
                    }
                };
            case ZIPFIAN:
            default:
                return new ScrambledZipfianGenerator(Long.MAX_VALUE);
        }
    }

}
