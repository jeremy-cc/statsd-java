package com.ccycloud.aws.statsd.impl;

import com.ccycloud.aws.statsd.StatsdClientInterface;
import com.ccycloud.aws.statsd.impl.logging.SystemLogger;
import com.ccycloud.aws.statsd.impl.transport.UdpConnectionInterface;

import java.util.AbstractQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;

/**
 * Concrete implementation of the statsd client
 *
 * Created by jeremyb on 03/04/2014.
 */
public class StatsdClient implements StatsdClientInterface{
    private UdpConnectionInterface connection;
    private Dispatcher dispatcher;

    private AbstractQueue<String> eventQueue = new ConcurrentLinkedQueue<String>();

    public final String TIMED_EVENT_FORMAT = "%s.%s%s%s:%d|ms";
    public final String GAUGED_EVENT_FORMAT = "%s.%s%s%s:%d|g";
    public final String SIMPLE_COUNT_FORMAT = "%s.%s%s%s:%d|c";
    public final String SAMPLED_COUNT_FORMAT = "%s.%s%s%s:%d|@%.3f";

    public final String TAG_BLOCK = "%s=%s,";
    // TCC-9027 do not enqueue new events if we do not have a connection established

    // if no tags are supplied the bucket and measurements are separated by whitespace, not a comma
    public final String TAGGED_MEASUREMENT_SEPARATOR = ",";
    public final String VANILLA_MEASUREMENT_SEPARATOR = " ";

    @Override
    public void incrementCounter(String prefix, String bucket, Map<String,String> tags, int count) {
        if(connection.isConnected()) {
            String sep = tags.size() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : VANILLA_MEASUREMENT_SEPARATOR;
            eventQueue.add(String.format(SIMPLE_COUNT_FORMAT, prefix, bucket, sep, join(tags), count));
        }
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, Map<String,String> tags, int count, double sampleRate) {
        if(connection.isConnected()) {
            String sep = tags.size() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : VANILLA_MEASUREMENT_SEPARATOR;
            eventQueue.add(String.format(SAMPLED_COUNT_FORMAT, prefix, bucket, count, sep, join(tags), sampleRate));
        }
    }

    @Override
    public void gaugeReading(String prefix, String bucket, Map<String,String> tags, int count) {
        if(connection.isConnected()) {
            String sep = tags.size() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : VANILLA_MEASUREMENT_SEPARATOR;
            eventQueue.add(String.format(GAUGED_EVENT_FORMAT, prefix, bucket, sep, join(tags), count));
        }
    }

    @Override
    public void timedEvent(String prefix, String bucket, Map<String,String> tags, int eventDurationInMs) {
        if(connection.isConnected()) {
            String sep = tags.size() > 0 ? TAGGED_MEASUREMENT_SEPARATOR : VANILLA_MEASUREMENT_SEPARATOR;
            eventQueue.add(String.format(TIMED_EVENT_FORMAT, prefix, bucket, sep, join(tags), eventDurationInMs));
        }
    }

    @Override
    public void incrementUniqueCounter(String prefix, String bucket, Map<String,String> tags, int count) {
        throw new RuntimeException("Not yet implemented.");
    }

    public StatsdClient(UdpConnectionInterface connection) {
        this.connection = connection;
    }

    public void startMeasurements() {
        dispatcher = new Dispatcher();
        dispatcher.start();
    }

    public void finishMeasurements() {
        dispatcher.shutdown();
    }

    private void dispatchAllEnqueuedEvents() {
        for(; !eventQueue.isEmpty();) {
            connection.send(eventQueue.remove());
        }
    }

    // join all keys in format k=v separated by commas
    private String join(Map<String,String> tags) {
        StringBuilder sb = new StringBuilder();
        for(String k : tags.keySet()) {
            sb.append(String.format(TAG_BLOCK, k, tags.get(k)));
        }
        String result = sb.toString();
        return result.endsWith(",") ? result.substring(0, result.length() - 1) : result;
    }

    class Dispatcher implements Runnable {
        private boolean running = false;
        private Thread thread;

        public synchronized void start() {
            running = true;
            thread = new Thread(this);
            thread.start();
        }

        public synchronized void shutdown() {
            running = false;
            try {
                dispatchAllEnqueuedEvents();
                thread.join();
            }catch(InterruptedException ie) {
                SystemLogger.error(ie.getMessage());
            }
        }

        /*
        * Sleep 100ms between attempts to poll the event queue if it empty*/
        public void run() {
            while(running){
                if(eventQueue.isEmpty()) {
                    try {
                        Thread.sleep(100l);
                    } catch (InterruptedException e) {
                        SystemLogger.error(e.getMessage());
                    }
                } else {
                    if(connection.isConnected()) {
                        // spool everything that's queued when possible
                        dispatchAllEnqueuedEvents();
                    } else {
                        try {
                            Thread.sleep(100l);
                        } catch (InterruptedException e) {
                            SystemLogger.error(e.getMessage());
                        }
                    }
                }
            }
        }
    }
}
