package com.yahoo.omid.transaction;

import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.Client;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.CompactorScanner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

public class TestCompactorScanner {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompactorScanner.class);

    private static final long TEST_TS = 1L;

    @DataProvider(name = "cell-retain-options")
    public Object[][] createCellRetainOptions() {
        return new Object[][]{
                {1, true}, {2, false},
        };
    }

    @Test(dataProvider = "cell-retain-options")
    public void testShouldRetainNonTransactionallyDeletedCellMethod(int optionIdx, boolean retainOption)
            throws Exception {

        // Create required mocks
        @SuppressWarnings("unchecked")
        ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
        InternalScanner internalScanner = mock(InternalScanner.class);
        CommitTable.Client ctClient = mock(CommitTable.Client.class);
        @SuppressWarnings("unchecked")
        Queue<Client> queue = mock(Queue.class);
        RegionCoprocessorEnvironment rce = mock(RegionCoprocessorEnvironment.class);
        HRegion hRegion = mock(HRegion.class);
        HRegionInfo hRegionInfo = mock(HRegionInfo.class);
        SettableFuture<Long> f = SettableFuture.<Long>create();

        // Wire required mock internals
        f.set(TEST_TS);
        when(ctClient.readLowWatermark()).thenReturn(f);
        when(ctx.getEnvironment()).thenReturn(rce);
        when(rce.getRegion()).thenReturn(hRegion);
        when(hRegion.getRegionInfo()).thenReturn(hRegionInfo);

        LOG.info("Testing when retain is {}", retainOption);
        try (CompactorScanner scanner = spy(new CompactorScanner(ctx,
                internalScanner,
                ctClient,
                queue,
                false,
                retainOption))) {

            // Different cell types to test
            KeyValue regularKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.Put);
            KeyValue deleteKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.Delete);
            KeyValue deleteColumnKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteColumn);
            KeyValue deleteFamilyKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteFamily);
            KeyValue deleteFamilyVersionKV = new KeyValue(Bytes.toBytes("test-row"), TEST_TS, Type.DeleteFamilyVersion);

            assertFalse(scanner.shouldRetainNonTransactionallyDeletedCell(regularKV));
            assertEquals(retainOption, scanner.shouldRetainNonTransactionallyDeletedCell(deleteKV));
            assertEquals(retainOption, scanner.shouldRetainNonTransactionallyDeletedCell(deleteColumnKV));
            assertEquals(retainOption, scanner.shouldRetainNonTransactionallyDeletedCell(deleteFamilyKV));
            assertEquals(retainOption, scanner.shouldRetainNonTransactionallyDeletedCell(deleteFamilyVersionKV));

        }

    }

}
