package com.yahoo.omid.notifications;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.AppSandbox.App;
import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer;
import com.yahoo.omid.notifications.conf.DeltaOmidServerConfig;

public class TestScannerSandbox extends TestInfrastructure {
    
    private static Logger logger = LoggerFactory.getLogger(TestScannerSandbox.class);

    private ScannerSandbox scannerSandbox;

    @Before
    public void setup() throws Exception {
        DeltaOmidServerConfig conf = DeltaOmidServerConfig.getDefaultConfig();
        scannerSandbox = new ScannerSandbox(conf);
    }

    @After
    public void teardown() throws Exception {

    }
    
    @Test
    public void testRegisterApplicationInterestsWithSuccess() throws Exception {
        Set<String> interests = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf2:c2"); } };
        App app = mock(App.class);
        when(app.getInterests()).thenReturn(interests);
        scannerSandbox.registerInterestsFromApplication(app);
        Map<String, ScannerContainer> scanners = scannerSandbox.getScanners();
        assertTrue(scanners.get("t1:cf1:c1").countInterestedApplications() == 1);
        assertTrue(scanners.get("t1:cf2:c2").countInterestedApplications() == 1);
    }
    
    @Test
    public void testRegisterInterestsOf2ApplicationsSequentiallyWithACommonInterest() throws Exception {
        Set<String> interestsApp1 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf2:c2"); } };
        Set<String> interestsApp2 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf1:c2"); } };
        App app1 = mock(App.class);
        App app2 = mock(App.class);
        when(app1.getInterests()).thenReturn(interestsApp1);
        when(app2.getInterests()).thenReturn(interestsApp2);
        scannerSandbox.registerInterestsFromApplication(app1);
        scannerSandbox.registerInterestsFromApplication(app2);
        Map<String, ScannerContainer> scanners = scannerSandbox.getScanners();
        assertTrue(scanners.get("t1:cf1:c1").countInterestedApplications() == 2);
        assertTrue(scanners.get("t1:cf2:c2").countInterestedApplications() == 1);
        assertTrue(scanners.get("t1:cf1:c2").countInterestedApplications() == 1);
    }
    
    
    @Test
    public void testRegisterInterestsOf2ApplicationsSequentiallyWithACommonInterestAndThenDeregisterTheInterestsOfOneOfThem() throws Exception {
        Set<String> interestsApp1 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf2:c2"); } };
        Set<String> interestsApp2 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf1:c2"); } };
        App app1 = mock(App.class);
        App app2 = mock(App.class);
        when(app1.getInterests()).thenReturn(interestsApp1);
        when(app2.getInterests()).thenReturn(interestsApp2);
        scannerSandbox.registerInterestsFromApplication(app1);
        scannerSandbox.registerInterestsFromApplication(app2);
        scannerSandbox.removeInterestsFromApplication(app1);
        Map<String, ScannerContainer> scanners = scannerSandbox.getScanners();
        assertTrue(scanners.get("t1:cf1:c1").countInterestedApplications() == 1);
        assertTrue(scanners.get("t1:cf2:c2") == null);
        assertTrue(scanners.get("t1:cf1:c2").countInterestedApplications() == 1);
    }
    
    @Test
    public void testRegisterInterestsOf3ApplicationsConcurrently() throws Exception {
        Set<String> interestsApp1 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf2:c2"); } };
        Set<String> interestsApp2 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf1:c2"); } };
        Set<String> interestsApp3 = new HashSet<String>() { { add("t1:cf1:c1") ; add("t1:cf1:c3"); } };
        final App app1 = mock(App.class);
        final App app2 = mock(App.class);
        final App app3 = mock(App.class);
        when(app1.getInterests()).thenReturn(interestsApp1);
        when(app2.getInterests()).thenReturn(interestsApp2);
        when(app3.getInterests()).thenReturn(interestsApp3);
        
        final Random randGen = new Random();
        
        final CountDownLatch startCdl = new CountDownLatch(1);
        final CountDownLatch endCdl = new CountDownLatch(3);
        
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    startCdl.await();
                    for (int i = 0; i < 50; i++) {
                        scannerSandbox.registerInterestsFromApplication(app1);
                        Thread.currentThread().sleep((long) randGen.nextInt(3) * 1000);
                        scannerSandbox.removeInterestsFromApplication(app1);
                    }
                    scannerSandbox.registerInterestsFromApplication(app1);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endCdl.countDown();
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    startCdl.await();
                    for (int i = 0; i < 50; i++) {
                        scannerSandbox.registerInterestsFromApplication(app2);
                        Thread.currentThread().sleep((long) randGen.nextInt(3) * 1000);
                        scannerSandbox.removeInterestsFromApplication(app2);
                    }
                    scannerSandbox.registerInterestsFromApplication(app2);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endCdl.countDown();
                }
            }
        });
        
        Thread t3 = new Thread(new Runnable() {
            public void run() {
                try {
                    startCdl.await();
                    for (int i = 0; i < 50; i++) {
                        scannerSandbox.registerInterestsFromApplication(app3);
                        Thread.currentThread().sleep((long) randGen.nextInt(3) * 1000);
                        scannerSandbox.removeInterestsFromApplication(app3);
                    }
                    scannerSandbox.registerInterestsFromApplication(app3);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endCdl.countDown();
                }
            }
        });
        
        t1.start();
        t2.start();
        t3.start();
        startCdl.countDown();
        endCdl.await();
        
        Map<String, ScannerContainer> scanners = scannerSandbox.getScanners();
        assertTrue(scanners.get("t1:cf1:c1").countInterestedApplications() == 3);
        assertTrue(scanners.get("t1:cf2:c2").countInterestedApplications() == 1);
        assertTrue(scanners.get("t1:cf1:c2").countInterestedApplications() == 1);
        assertTrue(scanners.get("t1:cf1:c3").countInterestedApplications() == 1);
    }
    
    
}
