/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso.persistence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TimestampOracle;

/**
 * Builds the TSO state from a BookKeeper ledger if there has been a previous incarnation of TSO. Note that we need to
 * make sure that the zookeeper session is the same across builder and logger, so we create in builder and pass it to
 * logger. This is the case to prevent two TSO instances from creating a lock and updating the ledger id after having
 * lost the lock. This case constitutes leads to an invalid system state.
 * 
 */

public class FileSystemTimestampOnlyStateBuilder extends StateBuilder {
    private static final Log LOG = LogFactory.getLog(FileSystemTimestampOnlyStateBuilder.class);

    public static TSOState getState(TSOServerConfig config) {
        TSOState returnValue;
        if (!config.isRecoveryEnabled()) {
            LOG.warn("Logger is disabled");
            returnValue = new TSOState(new TimestampOracle());
            returnValue.initialize();
        } else {
            FileSystemTimestampOnlyStateBuilder builder = new FileSystemTimestampOnlyStateBuilder(config);

            try {
                returnValue = builder.buildState();
                LOG.info("State built");
            } catch (Throwable e) {
                LOG.error("Error while building the state.", e);
                returnValue = null;
            } finally {
                builder.shutdown();
            }
        }
        return returnValue;
    }

    TimestampOracle timestampOracle;
    TSOServerConfig config;

    FileSystemTimestampOnlyStateBuilder(TSOServerConfig config) {
        this.timestampOracle = new TimestampOracle();
        this.config = config;
    }

    @Override
    public TSOState buildState() throws LoggerException {

        File directory = new File(config.getFsLog());
        TSOState state = new TSOState(timestampOracle);

        long startTimestamp = getTimestamp(directory);

        timestampOracle.initialize(startTimestamp);
        state.largestDeletedTimestamp = timestampOracle.first();

        state.setLogger(new FileSystemTimestampOnlyLogger(directory));
        state.getLogger().initialize(null, config);
        return state;
    }

    private long getTimestamp(File directory) {
        if (directory == null || !directory.exists()) {
            return 0;
        }
        long maxTimestamp = 0;
        if (directory.listFiles() != null) {
            for (File file : directory.listFiles()) {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    long currentTimestamp = Long.parseLong(reader.readLine());
                    maxTimestamp = Math.max(maxTimestamp, currentTimestamp);
                } catch (IOException e) {
                    LOG.error("Couldn't read timestamp", e);
                } catch (NumberFormatException e) {
                    LOG.error("Couldn't decode timestamp", e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            LOG.error(e);
                        }
                    }
                }
            }
        } else {
            if (!directory.exists()) {
                LOG.info("Directory " + directory.getAbsolutePath() + " does not exist, maxTimeStamp will be 0");
            } else {
                LOG.info("Directory " + directory.getAbsolutePath() + " cannot be read, maxTimeStamp will be 0");
            }
        }
        return maxTimestamp;
    }

    /**
     * Disables this builder.
     */
    @Override
    public void shutdown() {
    }
}
