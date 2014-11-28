package com.ml.ira;

import junit.framework.TestCase;

import java.util.List;

/**
 * Created by yaming_deng on 14-5-13.
 */
public class AppConfigTest extends TestCase {

    public void testCluster() throws Exception {
        AppConfig appConfig = new AppConfig("cluster");
        List ranges = appConfig.get("ranges");
        System.out.println(ranges);
    }
}
