package com.yahoo.omid.tso;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.Set;
import java.util.HashSet;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.CellId;

public class TestMassiveTransaction extends TSOTestBase {

    @Test(timeOut = 30000)
    public void testTransactionWithMassiveWriteSet() throws Exception {
        long startTs = client.getNewStartTimestamp().get();

        Set<CellId> cells = new HashSet<CellId>();
        for (int i = 0; i < 1000000; i++) {
            cells.add(new DummyCellIdImpl(i));
        }

        long commitTs = client.commit(startTs, cells).get();
        AssertJUnit.assertTrue("commit timestamp should be higher than start timestamp", commitTs > startTs);
    }
}
