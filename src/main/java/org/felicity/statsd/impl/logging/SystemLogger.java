package org.felicity.statsd.impl.logging;

/**
 * Basic spool to console
 * Created by jeremyb on 07/04/2014.
 */
public class SystemLogger {

    public static void info(String message) {
        System.out.println(message);
    }

    public static void error(String message) {
        System.err.println(message);
    }

    public static void error(String message, Exception exception) {
        System.err.println(message);
        System.err.println("Caused by: ");
        if(null != exception) {
            exception.printStackTrace(System.err);
        }
    }
}
