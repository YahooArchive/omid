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

/**
 * BookKeeper implementation of StateLogger.
 */

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerException.Code;

class FileSystemTimestampOnlyLogger implements StateLogger {
    private static final Log LOG = LogFactory.getLog(FileSystemTimestampOnlyLogger.class);

    private File directory;
    private Random random = new Random();

    /**
     * Constructor creates a zookeeper and a bookkeeper objects.
     */
    FileSystemTimestampOnlyLogger(File directory) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructing Logger");
        }
        this.directory = directory;
    }

    /**
     * Initializes this logger object to add records. Implements the initialize method of the StateLogger interface.
     * 
     * @param cb
     * @param ctx
     */

    @Override
    public void initialize(final LoggerInitCallback cb, Object ctx) throws LoggerException {
        TSOServerConfig config = (TSOServerConfig) ctx;

        this.directory = new File(config.getFsLog());
        if (this.directory.exists())
            return;
        if (this.directory.mkdirs())
            return;
        throw LoggerException.create(Code.INITLOCKFAILED);
    }

    /**
     * Adds a record to the log of operations. The record is a byte array.
     * 
     * @param record
     * @param cb
     * @param ctx
     */
    @Override
    public void addRecord(byte[] record, final AddRecordCallback cb, Object ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding record.");
        }

        Long timestampOracle = getLatestTimestampOracle(record);
        if (timestampOracle == null) {
            cb.addRecordComplete(Code.OK, ctx);
            return;
        }

        if (writeToDisk(timestampOracle)) {
            cb.addRecordComplete(Code.OK, ctx);
        } else {
            cb.addRecordComplete(Code.BKOPFAILED, ctx);
        }

    }

    private boolean writeToDisk(long timestampOracle) {
        File[] existingFiles = directory.listFiles();
        File newFile = getUniqueFile(existingFiles);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));
            writer.write(Long.toString(timestampOracle));
            writer.close();
        } catch (IOException e) {
            LOG.error("Couldn't write timestamp", e);
            return false;
        }
        for (File file : existingFiles) {
            file.delete();
        }
        return true;
    }

    private File getUniqueFile(File[] existingFiles) {
        while (true) {
            String name = Integer.toString(random.nextInt(Integer.MAX_VALUE));
            boolean exists = false;
            for (File file : existingFiles) {
                if (file.getName().equals(name)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                return new File(directory, name);
            }
        }
    }

    private Long getLatestTimestampOracle(byte[] record) {
        DataInputStream reader = new DataInputStream(new ByteArrayInputStream(record));
        Long timestampOracle = null;
        try {
            while (true) {
                byte type;
                try {
                    type = reader.readByte();
                } catch (EOFException e) {
                    break;
                }
                switch (type) {
                case LoggerProtocol.TIMESTAMPORACLE:
                    timestampOracle = reader.readLong();
                    break;
                // ignore all other cases, skipping 0, 1 or 2 longs
                case LoggerProtocol.COMMIT:
                    reader.readLong();
                case LoggerProtocol.ABORT:
                case LoggerProtocol.FULLABORT:
                case LoggerProtocol.SNAPSHOT:
                case LoggerProtocol.LARGESTDELETEDTIMESTAMP:
                    reader.readLong();
                case LoggerProtocol.LOGSTART:
                    break;
                }
            }
        } catch (IOException e) {
            LOG.error("Couldn't parse record correctly", e);
            LOG.error("Record that caused it: " + Arrays.toString(record));
        }
        return timestampOracle;
    }

    /**
     * Shuts down this logger.
     * 
     */
    public void shutdown() {

    }

}
