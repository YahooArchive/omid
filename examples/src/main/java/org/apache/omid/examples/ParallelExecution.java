/*
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
package org.apache.omid.examples;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.transaction.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * ****************************************************************************************************************
 *
 * Same as SnapshotIsolationExample only executes multiple transactions concurrently
 *
 * ****************************************************************************************************************
 *
 * Please @see{SnapshotIsolationExample}
 */
public class ParallelExecution {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelExecution.class);
    private static final long heartBeatInterval = 10_000;

    public static void main(String[] args) throws Exception {

        LOG.info("Parsing the command line arguments");
        int maxThreads = Runtime.getRuntime().availableProcessors();
        if (args != null && args.length > 2 && StringUtils.isNotEmpty(args[2])) {
            maxThreads = Integer.parseInt(args[2]);
        }
        LOG.info("Execute '{}' concurrent threads", maxThreads);

        for (int i = 0; i < maxThreads; i++) {
            final SnapshotIsolationExample example = new SnapshotIsolationExample(args);
            example.setRowIdGenerator(new RandomRowIdGenerator());
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    long lastHeartBeatTime = System.currentTimeMillis();
                    long counter = 0;
                    long errorCounter = 0;
                    while (true) {
                        LOG.info("New cycle starts");
                        try {
                            example.execute();
                            counter++;
                        } catch (IOException | RollbackException | IllegalStateException e) {
                            LOG.error("", e);
                            errorCounter++;
                        }
                        if (System.currentTimeMillis() > lastHeartBeatTime + heartBeatInterval) {
                            LOG.error(String.format("%s cycles executed, %s errors", counter, errorCounter));
                            lastHeartBeatTime = System.currentTimeMillis();
                        }
                    }
                }
            });
            t.setName(String.format("SnapshotIsolationExample thread %s/%s", i + 1, maxThreads));
            t.start();
        }

    }

    private static class RandomRowIdGenerator implements RowIdGenerator {

        private Random random = new Random();

        @Override
        public byte[] getRowId() {
            return Bytes.toBytes("id" + random.nextInt());
        }
    }


}
