package com.yahoo.omid.timestamp.storage;

import java.io.IOException;

public interface TimestampStorage {

    String TIMESTAMPSTORAGE_TABLE_NAME_KEY = "omid.timestampstorage.tablename";

    void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException;

    long getMaxTimestamp() throws IOException;

}
