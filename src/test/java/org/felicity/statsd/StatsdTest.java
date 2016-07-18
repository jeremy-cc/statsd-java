package org.felicity.statsd;

import junit.framework.TestCase;
import org.felicity.statsd.impl.config.MissingConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jeremyb on 03/04/2014.
 */
@RunWith(MockitoJUnitRunner.class)
public class StatsdTest extends TestCase {

    @Mock
    private org.felicity.statsd.impl.StatsdClient client;

    @Before
    public void init() {

    }

//    @Test
//    public void testGetInstanceWithConfig() throws Exception{
//
//        Map<String,String> configuration = new HashMap<String,String>();
//        configuration.put("statsd.host", "localhost");
//        configuration.put("statsd.port", "8080");
//
//        StatsdClient instance = StatsdClient.getInstance(configuration);
//        assertNotNull(instance);
//    }

    @Test
    public void testActualConnection() throws Exception {
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("statsd.host", "localhost");
        configuration.put("statsd.port", "8125");

        StatsdClient instance = StatsdClient.getInstance(configuration);

        HashMap<String,String> tmp = new HashMap<String,String>();
        tmp.put("provider", "OANDA");
        tmp.put("server", "vagrant");

        instance.incrementCounter("testing", "test_counter", tmp, 1);

        Thread.sleep(1000l);

    }
}
