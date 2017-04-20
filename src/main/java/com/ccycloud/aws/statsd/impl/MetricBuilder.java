package com.ccycloud.aws.statsd.impl;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Combines app names, prefixes, tags and separators into a sane and parseable statsd string
 * Created by jeremybotha on 20/04/17.
 */
public class MetricBuilder {

    public final String TIMED_EVENT_FORMAT = "%s%s%s:%d|ms";
    public final String GAUGED_EVENT_FORMAT = "%s%s%s:%d|g";
    public final String SIMPLE_COUNT_FORMAT = "%s%s%s:%d|c";
    public final String SAMPLED_COUNT_FORMAT = "%s%s%s:%d|c@%.3f";

    public final String BUCKET_BLOCK = "%s.%s";
    public final String TAG_BLOCK = "%s=%s,";
    // TCC-9027 do not enqueue new events if we do not have a connection established

    // if no tags are supplied the bucket and measurements are separated by whitespace, not a comma
    public final String TAGGED_MEASUREMENT_SEPARATOR = ",";
    public final String PLAIN_MEASUREMENT_SEPARATOR = "";

    private Pattern leadingPattern = Pattern.compile("^\\W+");
    private Pattern trailingPattern = Pattern.compile("\\W+$");
    private Pattern duplicateSeparatorPattern = Pattern.compile("\\.+");

    public String buildCounter(String appname, String measurement, Map<String,String> tags, int count) {
        String tagstring = join(tags);
        String separator = tagstring.length() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : PLAIN_MEASUREMENT_SEPARATOR;
        String bucket = buildBucket(appname, measurement);

        return String.format(SIMPLE_COUNT_FORMAT, bucket, separator, tagstring, count);
    }

    public String buildGauge(String appname, String measurement, Map<String,String> tags, int gaugeReading) {
        String tagstring = join(tags);
        String separator = tagstring.length() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : PLAIN_MEASUREMENT_SEPARATOR;
        String bucket = buildBucket(appname, measurement);

        return String.format(GAUGED_EVENT_FORMAT, bucket, separator, tagstring, gaugeReading);
    }

    public String buildTimer(String appname, String measurement, Map<String,String> tags, int reading) {
        String tagstring = join(tags);
        String separator = tagstring.length() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : PLAIN_MEASUREMENT_SEPARATOR;
        String bucket = buildBucket(appname, measurement);

        return String.format(TIMED_EVENT_FORMAT, bucket, separator, tagstring, reading);
    }

    public String buildSampleCounter(String appname, String measurement, Map<String,String> tags, int reading, double frequency) {
        String tagstring = join(tags);
        String separator = tagstring.length() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : PLAIN_MEASUREMENT_SEPARATOR;
        String bucket = buildBucket(appname, measurement);

        return String.format(SAMPLED_COUNT_FORMAT, bucket, separator, tagstring, reading, frequency);
    }

    public String buildBucket(String appname, String measurement) {
        String _appname = sanitise(appname);
        String _measurement = sanitise(measurement);

        if(_appname.length() == 0) {
            return _measurement;
        } else {
            if(_measurement.length() == 0) {
                return _appname;
            } else {
                return String.format(BUCKET_BLOCK, _appname, _measurement);
            }
        }
    }

    public String sanitise(String src) {
        if(null == src) {
            return "";
        }
        String working = src.replaceAll("\\s+", "");

        // trim leading commas
        Matcher m = leadingPattern.matcher(working);

        if(m.find()) {
            working = m.replaceFirst("");
        }

        // trim tailing commas
        m = trailingPattern.matcher(working);
        if(m.find()) {
           working = m.replaceFirst("");
        }

        m = duplicateSeparatorPattern.matcher(working);
        if(m.find()) {
            working = m.replaceAll(".");
        }

        return working.trim();
    }


    // join all keys in format k=v separated by commas
    public String join(Map<String,String> tags) {
        StringBuilder sb = new StringBuilder();
        String result;

        if(null != tags && tags.size() > 0) {
            for (String k : tags.keySet()) {
                sb.append(String.format(TAG_BLOCK, sanitise(k), sanitise(tags.get(k))));
            }
            result = sb.toString();
            return result.endsWith(",") ? result.substring(0, result.length() - 1) : result;
        } else {
            result = "";
        }
        return result;
    }

}
