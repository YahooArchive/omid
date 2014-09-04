package com.yahoo.omid.timestamp.storage;

import java.io.IOException;

public interface TimestampStorage {

    public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException;

    public long getMaxTimestamp() throws IOException;

}
