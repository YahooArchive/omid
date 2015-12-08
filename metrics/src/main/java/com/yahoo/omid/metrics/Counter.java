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