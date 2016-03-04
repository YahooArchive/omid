package com.yahoo.omid;


import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class YAMLUtilsTest {

    @Test
    public void testLoadDefaultSettings() throws Exception {
        Map map = new YAMLUtils<Map>().loadDefaultSettings(Collections.singletonList("test.yml"));
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 1);
        Assert.assertEquals(map.get("prop2"), "2");
    }

    @Test
    public void testLoadDefaultSettings_setToBean() throws Exception {
        Map map = new HashMap();
        new YAMLUtils<Map>().loadDefaultSettings(Collections.singletonList("test.yml"), map);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 1);
        Assert.assertEquals(map.get("prop2"), "2");
    }

}