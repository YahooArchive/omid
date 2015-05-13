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
package com.yahoo.omid.metrics;

public interface Counter extends Metric {

        /**
         * Increment the counter by one.
         */
        public void inc();

        /**
         * Increment the counter by {@code n}.
         *
         * @param n the amount by which the counter will be increased
         */
        public void inc(long n);

        /**
         * Decrement the counter by one.
         */
        public void dec();

        /**
         * Decrement the counter by {@code n}.
         *
         * @param n the amount by which the counter will be decreased
         */
        public void dec(long n);

}