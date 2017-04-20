package com.ccycloud.aws.statsd.impl;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.HashMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetricBuilderTest  extends TestCase {

    private MetricBuilder subject;

    @Before
    public void init() throws IOException {
        subject = new MetricBuilder();
    }

    @Test
    public void testBuildCounter() {
        String expected = "fixengine-barclays.fix_engine.Barclays.trades.heartbeat:1|c";
        String actual = subject.buildCounter("fixengine-barclays.", ".fix_engine.Barclays.trades.heartbeat .", null, 1);

        assertEquals("Verify generated string matches expected", expected, actual);

        expected = "fixengine-barclays:1|c";
        actual = subject.buildCounter("fixengine-barclays.", "...", null, 1);
        assertEquals("Verify generated string matches expected", expected, actual);
    }

    @Test
    public void testBuildGauge() {
        String expected = "fixengine-barclays.fix_engine.Barclays.trades.heartbeat:1|g";
        String actual = subject.buildGauge("fixengine-barclays.", ".fix_engine.Barclays.trades.heartbeat .", null, 1);

        assertEquals("Verify generated string matches expected", expected, actual);

        expected = "fixengine-barclays:1|g";
        actual = subject.buildGauge("fixengine-barclays.", "...", null, 1);
        assertEquals("Verify generated string matches expected", expected, actual);
    }

    @Test
    public void testBuildTimer() {
        String expected = "fixengine-barclays.fix_engine.Barclays.trades.heartbeat:1|ms";
        String actual = subject.buildTimer("fixengine-barclays.", ".fix_engine.Barclays.trades.heartbeat .", null, 1);

        assertEquals("Verify generated string matches expected", expected, actual);

        expected = "fixengine-barclays:1|ms";
        actual = subject.buildTimer("fixengine-barclays.", "...", null, 1);
        assertEquals("Verify generated string matches expected", expected, actual);
    }

    @Test
    public void testBuildSampledCounter() {
        String expected = "fixengine-barclays.fix_engine.Barclays.trades.heartbeat:1|c@0.200";
        String actual = subject.buildSampleCounter("fixengine-barclays.", ".fix_engine.Barclays.trades.heartbeat .", null, 1, 0.2);

        assertEquals("Verify generated string matches expected", expected, actual);

        expected = "fixengine-barclays:1|c@0.200";
        actual = subject.buildSampleCounter("fixengine-barclays.", "...", null, 1, 0.2);
        assertEquals("Verify generated string matches expected", expected, actual);
    }

    @Test
    public void testSanitiseCommas() {
        assertEquals("Trailing commas should be stripped", "abc", subject.sanitise("abc,"));
        assertEquals("Leading commas should be stripped", "abc", subject.sanitise(",abc,"));
        assertEquals("Bracketing commas should be stripped", "abc", subject.sanitise(",,abc,,"));
        assertEquals("Bracketing commas should be stripped", "abc", subject.sanitise(", ,abc, ,"));
    }

    @Test
    public void testSanitiseWhitespace() {
        assertEquals("Leading whitespace should be stripped", "abc", subject.sanitise("  abc"));
        assertEquals("Trailing whitespace should be stripped", "abc", subject.sanitise("  abc  "));
    }

    @Test
    public void testDuplicateSeparatorsRemoved() {
        assertEquals("Multiple separators are suppressed", "a.b.c", subject.sanitise("a..b.c"));
        assertEquals("Multiple separators are suppressed", "a.b.c", subject.sanitise("a..b..c"));
        assertEquals("Multiple inner and outer separators are suppressed", "a.b.c", subject.sanitise("..a..b.c.."));
    }

    @Test
    public void testOutOfPlaceSeparatorsRemoved() {
        assertEquals("Leading separators are suppressed", "a.b.c", subject.sanitise(".a.b.c"));
        assertEquals("Trailing separators are suppressed", "a.b.c", subject.sanitise("a.b.c."));
        assertEquals("Multiple separators are suppressed", "a.b.c", subject.sanitise("a.b.c.."));
    }


    @Test
    public void testEmptyHashJoin() {
        HashMap<String,String> source = new HashMap<String,String>();
        assertEquals("An empty hash should generate an empty string", "", subject.join(source));
    }

    @Test
    public void testNullHashJoin() {
        assertEquals("A null hash should generate an empty string", "", subject.join(null));
    }

    @Test
    public void testKeyNamesSanitised() {
        HashMap<String,String> source = new HashMap<String,String>();
        source.put("k2,", "v3");

        assertEquals("Commas should be stripped from keys", "k2=v3", subject.join(source));
    }

    @Test
    public void testKeyValuesSanitised() {
        HashMap<String,String> source = new HashMap<String,String>();
        source.put("k2", "v2,");

        assertEquals("Commas should be stripped from values", "k2=v2", subject.join(source));
    }

    @Test
    public void testJoinLengths() {
        HashMap<String,String> source = new HashMap<String,String>();
        source.put("k1", "v1");
        assertEquals("A hash of length 1 should generate an undelimited string", "k1=v1", subject.join(source));

        source.put("k2", "v2");
        assertEquals("A hash of length > 1 should generate a delimited string", "k1=v1,k2=v2", subject.join(source));
    }

    @Test
    public void testNullAppnameBuildBucket() {
        String measurement = ".r1..r2. .r3  ..";

        assertEquals("A null appname should be suppressed so just the measurement is returned",
                "r1.r2.r3",
                subject.buildBucket(null, measurement));
    }

    @Test
    public void testNullMeasurementBuildBucket() {
        String appname = "appname";

        assertEquals("A null measurement should be suppressed so just the appname is returned",
                "appname",
                subject.buildBucket(appname, null));
    }

    @Test
    public void testBuildBucket() {
        String appname = "appname";
        String measurement = ".r1..r2. .r3  ..";

        assertEquals("A bucket should be a sanitised app and measurement",
                "appname.r1.r2.r3",
                subject.buildBucket(appname, measurement));
    }

}