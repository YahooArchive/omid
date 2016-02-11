package com.yahoo.omid.transaction;

import com.yahoo.omid.tsoclient.TSOClient;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "sharedHBase")
public class TestHBaseTransactionManager extends OmidTestBase {

    private static final int FAKE_EPOCH_INCREMENT = 100;

    @Test(timeOut = 20_000)
    public void testTxManagerGetsTimestampsInTheRightEpoch(ITestContext context) throws Exception {

        TSOClient tsoClient = spy(getClient(context));

        long fakeEpoch = tsoClient.getNewStartTimestamp().get() + FAKE_EPOCH_INCREMENT;

        // Modify the epoch before testing the begin method
        doReturn(fakeEpoch).when(tsoClient).getEpoch();

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, tsoClient));

        // Create a transaction with the initial setup and check that the TX id matches the fake epoch created
        Transaction tx1 = tm.begin();
        assertEquals(fakeEpoch, tx1.getTransactionId());
        verify(tsoClient, timeout(100).times(FAKE_EPOCH_INCREMENT)).getEpoch();

    }

}
