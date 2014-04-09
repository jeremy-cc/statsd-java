package org.felicity.statsd;

/**
 * Defines the basic interface for the statsd subsystem.
 *
 * StatsdClientInterface's protocol is defined here: https://github.com/etsy/statsd/blob/master/docs/metric_types.md
 *
 * Created by jeremyb on 03/04/2014.
 */
public interface StatsdClientInterface {
    public void incrementCounter(String prefix, String bucket, int count);
    public void incrementSampleCounter(String prefix, String bucket, int count, double sampleRate);
    public void gaugeReading(String prefix, String bucket, int count);
    public void timedEvent(String prefix, String bucket, int eventDurationInMs);
    public void incrementUniqueCounter(String prefix, String bucket, int count);
}
