package org.felicity.statsd.impl;

import org.felicity.statsd.StatsdClientInterface;
import org.felicity.statsd.impl.logging.SystemLogger;
import org.felicity.statsd.impl.transport.UdpConnectionInterface;

import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Concrete implementation of the statsd client
 *
 * Created by jeremyb on 03/04/2014.
 */
public class StatsdClient implements StatsdClientInterface{
    private UdpConnectionInterface connection;
    private Dispatcher dispatcher;

    private AbstractQueue<String> eventQueue = new ConcurrentLinkedQueue<String>();

    public final String TIMED_EVENT_FORMAT = "%s.%s,%s:%d|ms";
    public final String GAUGED_EVENT_FORMAT = "%s.%s,%s:%d|g";
    public final String SIMPLE_COUNT_FORMAT = "%s.%s,%s:%d|c";
    public final String SAMPLED_COUNT_FORMAT = "%s.%s,%s:%d|@%.3f";

    public final String TAG_BLOCK = "%s=%s";

    @Override
    public void incrementCounter(String prefix, String bucket, HashMap<String,String> tags, int count) {

        eventQueue.add(String.format(SIMPLE_COUNT_FORMAT, prefix, bucket, join(tags), count));
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, HashMap<String,String> tags, int count, double sampleRate) {
        eventQueue.add(String.format(SAMPLED_COUNT_FORMAT, prefix, bucket, count, join(tags), sampleRate));
    }

    @Override
    public void gaugeReading(String prefix, String bucket, HashMap<String,String> tags, int count) {
        eventQueue.add(String.format(GAUGED_EVENT_FORMAT, prefix, bucket, join(tags), count));
    }

    @Override
    public void timedEvent(String prefix, String bucket, HashMap<String,String> tags, int eventDurationInMs) {
        eventQueue.add(String.format(TIMED_EVENT_FORMAT, prefix, bucket, join(tags), eventDurationInMs));
    }

    @Override
    public void incrementUniqueCounter(String prefix, String bucket, HashMap<String,String> tags, int count) {
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

    private String join(HashMap<String,String> tags) {
        if(null == tags || tags.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for(String k : tags.keySet()) {
            sb.append(k).append("=").append(tags.get(k)).append(",");
        }
        return sb.substring(0, sb.length()-1); // omit final comma
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
                    // spool everything that's queued when possible
                    dispatchAllEnqueuedEvents();
                }
            }
        }
    }
}
