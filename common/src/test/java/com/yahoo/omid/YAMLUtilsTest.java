package com.yahoo.omid;


import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class YAMLUtilsTest {

    @Test
    public void testLoadDefaultSettings_setToBean() throws Exception {
        Map map = new HashMap();
        new YAMLUtils().loadSettings("test.yml", "default-test.yml", map);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 11);
        Assert.assertEquals(map.get("prop2"), "22");
        Assert.assertEquals(map.get("prop3"), 3);
    }

    @Test
    public void testLoadDefaultSettings_setToBean2() throws Exception {
        Map map = new HashMap();
        new YAMLUtils().loadSettings("test.yml", map);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.get("prop1"), 11);
        Assert.assertEquals(map.get("prop2"), "22");
        Assert.assertEquals(map.size(), 2);
    }

}