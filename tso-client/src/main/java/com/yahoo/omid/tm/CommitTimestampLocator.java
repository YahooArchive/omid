package com.yahoo.omid.tm;

import java.io.IOException;

import com.google.common.base.Optional;

/**
 * An behavior that needs to be implemented by transaction managers 
 * to try to locate the possible commit timestamp for a cell. It
 * allows to the transaction managers implementing this interface to
 * search the commit timestamp in a local cache of their own or in 
 * their particular shadow cells implementation.
 */
public interface CommitTimestampLocator {

    public Optional<Long> readCommitTimestampFromCache(long startTimestamp);
    public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp)
            throws IOException;

}
