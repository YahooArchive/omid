package com.yahoo.omid.tso;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import com.yahoo.omid.tso.persistence.FileSystemTimestampOnlyStateBuilder;

public class TestFilesystemLogger {

    @Test
    public void testBigStartTimestamp() throws IOException {
        File directory = File.createTempFile("omid-test-logger", Long.toString(System.nanoTime()));
        directory.delete();
        directory.mkdir();
        File wal = new File(directory, "487384934");
        BufferedWriter writer = new BufferedWriter(new FileWriter(wal));
        writer.write(Long.toString(1823800054));
        writer.close();
        
        int uncommitted = 100;

        String args = "-port 1245 -ha -fsLog " + directory.getAbsolutePath();
        TSOServerConfig config = TSOServerConfig.parseConfig(args.split(" "));
        TSOState state = FileSystemTimestampOnlyStateBuilder.getState(config);
        long startTimestamp = state.getSO().get() + 1;
        for (int i = 0; i < uncommitted; ++i) {
            state.uncommited.start(startTimestamp + i);
        }
        Set<Long> toAbort = state.uncommited.raiseLargestDeletedTransaction(startTimestamp + uncommitted);
        assertEquals(uncommitted, toAbort.size());
    }
}
