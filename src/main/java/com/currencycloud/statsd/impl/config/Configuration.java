package com.currencycloud.statsd.impl.config;

/**
 * Created by jeremyb on 03/04/2014.
 */
public class Configuration {

    public static final String CONFIG_HOST = "statsd.host";
    public static final String CONFIG_PORT = "statsd.port";

    private static String[] configKeys = {
        CONFIG_HOST,
        CONFIG_PORT
};

    public static String[] getMandatoryConfigKeys() {
        return configKeys;
    }
}
