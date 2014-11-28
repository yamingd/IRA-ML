package com.ml.ira;

import java.lang.instrument.Instrumentation;

/**
 * Created by yaming_deng on 14-4-30.
 */
public class ObjectSizeFetcher {
    private static Instrumentation instrumentation;

    public static void premain(String args, Instrumentation inst) {
        instrumentation = inst;
    }

    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }
}
