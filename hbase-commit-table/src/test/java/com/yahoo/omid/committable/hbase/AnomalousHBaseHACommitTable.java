package com.yahoo.omid.committable.hbase;

import java.io.IOException;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Class just for testing purposes.
 *
 * Contains an anomalous writer that writes to the log but not
 * to the commit table.
 */
@VisibleForTesting
public class AnomalousHBaseHACommitTable extends HBaseHACommitTable {

    public AnomalousHBaseHACommitTable(Configuration hbaseConfig,
                                     HBaseCommitTableConfig config,
                                     BookKeeper bk,
                                     MetadataStorage metadataStore) {
        super(hbaseConfig, config, bk, metadataStore);
    }

    @VisibleForTesting
    public class AnomalousHBaseWriter extends HBaseWriter {

        AnomalousHBaseWriter(Configuration hbaseConfig, String tableName)
        throws IOException {
            super(hbaseConfig, tableName);
        }

        @Override
        public ListenableFuture<Void> flush() {
            SettableFuture<Void> f = SettableFuture.<Void> create();
            f.set(null);
            return f;
        }

    }

}
