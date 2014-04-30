package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;

public class HBaseCommitTable implements CommitTable {
    
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTable.class);
    
    static final byte[] COMMIT_TABLE_FAMILY = "F".getBytes(UTF_8);
    static final byte[] COMMIT_TABLE_QUALIFIER = "C".getBytes(UTF_8);
    
    private final HTable table;

    public HBaseCommitTable(HTable table) {
        this.table = table;
        table.setAutoFlush(false, true);
    }

    public class HBaseWriter implements Writer {

        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            assert(startTimestamp < commitTimestamp);
            
            Put put = new Put(Bytes.toBytes(startTimestamp));
            put.add(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER, Bytes.toBytes(commitTimestamp));
            
            table.put(put);
        }

        @Override
        public ListenableFuture<Void> flush() {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            try {
		table.flushCommits();
		f.set(null);
	    } catch (IOException e) {
		LOG.error("Error flushing data", e);
		f.setException(e);
	    }            
            return f;
        }

    }

    public class HBaseClient implements Client {

        @Override
        public ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp) {
            Get get = new Get(Bytes.toBytes(startTimestamp));
            get.addColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER);

            SettableFuture<Optional<Long>> f = SettableFuture.<Optional<Long>>create();
            Result result = null;
            try {
        	result = table.get(get);
        	if (containsATimestamp(result)) {
        	    long commitTs = Bytes.toLong(result.getValue(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
        	    Optional<Long> commitTsValue = Optional.of(commitTs);
        	    f.set(commitTsValue);
        	} else {
        	    Optional<Long> absentValue = Optional.absent();
        	    f.set(absentValue);
        	}
            } catch(IOException e) {
        	LOG.error("Error getting commit timestamp for TX {}", startTimestamp, e);
        	f.setException(e);
            }            
            return f;
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            Delete delete = new Delete(Bytes.toBytes(startTimestamp));
            
            SettableFuture<Void> f = SettableFuture.<Void>create();
            try {
        	table.delete(delete);
        	f.set(null);
            } catch(IOException e) {
        	LOG.error("Error removing TX {}", startTimestamp, e);
        	f.setException(e);
            }            
            return f;
        }
        
        private boolean containsATimestamp(Result result) {
            return (result != null && result.containsColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
        }

    }

    @Override
    public ListenableFuture<Writer> getWriter() {
	SettableFuture<Writer> f = SettableFuture.<Writer>create();
        f.set(new HBaseWriter());
        return f;
    }

    @Override
    public ListenableFuture<Client> getClient() {
	SettableFuture<Client> f = SettableFuture.<Client>create();
        f.set(new HBaseClient()); // Check this depending on (*)
        return f;
    }

}
