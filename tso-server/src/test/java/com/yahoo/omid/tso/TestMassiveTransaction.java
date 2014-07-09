package com.yahoo.omid.tso;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.Set;
import java.util.HashSet;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.CellId;

public class TestMassiveTransaction extends TSOTestBase {

    @Test(timeout=10000)
    public void testMassiveTransaction() throws Exception {
        long ts = client.getNewStartTimestamp().get();

        Set<CellId> cells = new HashSet<CellId>();
        for (int i = 0; i < 1000000; i++) {
            cells.add(new DummyCellIdImpl(i));
        }

        long commitTs = client.commit(ts, cells).get();
        assertTrue("commit timestamp should be higher than start timestamp", commitTs > ts);
    }
}
