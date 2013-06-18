package com.yahoo.omid.tso;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TestUncommitted extends TSOTestBase {

    @Test
    public void testManyBegins() throws IOException {
        int size = state.uncommited.getBucketSize();
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        Set<Long> toAbort = new HashSet<Long>();
        toAbort.add(clientHandler.receiveMessage(TimestampResponse.class).timestamp);
        for (int i = 0; i < size * 2; ++i) {
            clientHandler.sendMessage(new TimestampRequest());
            toAbort.add(clientHandler.receiveMessage(TimestampResponse.class).timestamp);
        }

        RowKey[] rows = new RowKey[] { new RowKey(Bytes.toBytes("row"), Bytes.toBytes("table")) };

        for (int i = 0; i < TSOState.MAX_COMMITS * 2; ++i) {
            clientHandler.sendMessage(new TimestampRequest());
            Object msg;
            do {
                msg = clientHandler.receiveMessage();
                if (msg instanceof AbortedTransactionReport) {
                    AbortedTransactionReport atr = (AbortedTransactionReport) msg;
                    toAbort.remove(atr.startTimestamp);
                } else if (msg instanceof TimestampResponse) {
                    break;
                }
            } while (true);
            TimestampResponse tr = (TimestampResponse) msg;
            clientHandler.sendMessage(new CommitRequest(tr.timestamp, rows));
            CommitResponse cr = clientHandler.receiveMessage(CommitResponse.class);
            assertTrue(cr.committed);
        }

        assertTrue("Uncommitted transactions weren't aborted", toAbort.isEmpty());
    }

    @Test
    public void testOverlapingBegins() throws IOException {
        int size = state.uncommited.getBucketSize() * state.uncommited.getBucketNumber();
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        Set<Long> aborted = new HashSet<Long>();
        TimestampResponse first = clientHandler.receiveMessage(TimestampResponse.class);
        for (int i = 0; i < size * 10; ++i) {
            clientHandler.sendMessage(new TimestampRequest());
            do {
                Object msg = clientHandler.receiveMessage();
                if (msg instanceof AbortedTransactionReport) {
                    AbortedTransactionReport atr = (AbortedTransactionReport) msg;
                    aborted.add(atr.startTimestamp);
                } else if (msg instanceof TimestampResponse) {
                    break;
                }
            } while (true);
        }

        RowKey[] rows = new RowKey[] { new RowKey(Bytes.toBytes("row"), Bytes.toBytes("table")) };
        clientHandler.sendMessage(new CommitRequest(first.timestamp, rows));
        CommitResponse response = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(response.committed);
    }
}
