package com.yahoo.omid.tso;

import static org.testng.Assert.assertEquals;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.yahoo.omid.tso.TSOServer.BaseOperatingSystem;

public class TestBaseOperatingSystem {

    private static final String FAKE_PROPERTY = "non-existing-property";

    @Test
    public void testGetTheRightOSForEveryEnvironment() {

        // Emulate linux environment
        BaseOperatingSystem resultingOS = BaseOperatingSystem.get(FAKE_PROPERTY, "Linux");
        assertEquals(BaseOperatingSystem.Linux, resultingOS);

        // Emulate mac environment
        resultingOS = BaseOperatingSystem.get(FAKE_PROPERTY, "Mac");
        assertEquals(resultingOS, BaseOperatingSystem.Mac);

        // Emulate an unknown environment
        try {
            resultingOS = BaseOperatingSystem.get(FAKE_PROPERTY, "Unknown");
            Assert.fail();
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        // Finally test the current environment where the tests are executing
        String baseOS = System.getProperty(TSOServer.OS_SYSTEM_PROPERTY);
        resultingOS = BaseOperatingSystem.get();
        assertEquals(resultingOS, BaseOperatingSystem.get(FAKE_PROPERTY, baseOS));

  }

}
