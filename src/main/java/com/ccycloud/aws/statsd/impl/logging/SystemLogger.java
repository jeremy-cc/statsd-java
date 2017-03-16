package com.ccycloud.aws.statsd.impl.logging;

/**
 * Basic spool to console
 * Created by jeremyb on 07/04/2014.
 */
public class SystemLogger {

    final static String PREFIX = "com.ccycloud.aws.statsd";

    public static void info(String message) {
        System.out.println(String.format("%s: %s", PREFIX, message));
    }

    public static void error(String message) {
        System.err.println(String.format("%s: %s", PREFIX, message));
    }

    public static void error(String message, Exception exception) {
        System.err.println(String.format("%s: %s", PREFIX, message));
        System.err.println("Caused by: ");
        if(null != exception) {
            exception.printStackTrace(System.err);
        }
    }
}
