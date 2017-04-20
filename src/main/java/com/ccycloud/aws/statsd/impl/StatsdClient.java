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
    private MetricBuilder builder = new MetricBuilder();

    private AbstractQueue<String> eventQueue = new ConcurrentLinkedQueue<String>();

    @Override
    public void incrementCounter(String prefix, String bucket, Map<String,String> tags, int count) {
        if(connection.isConnected()) {
            eventQueue.add(builder.buildCounter(prefix, bucket, tags, count));
        }
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, Map<String,String> tags, int count, double sampleRate) {
        if(connection.isConnected()) {
            eventQueue.add(builder.buildSampleCounter(prefix, bucket, tags, count, sampleRate));
        }
    }

    @Override
    public void gaugeReading(String prefix, String bucket, Map<String,String> tags, int count) {
        if(connection.isConnected()) {
            eventQueue.add(builder.buildGauge(prefix, bucket, tags, count));
        }
    }

    @Override
    public void timedEvent(String prefix, String bucket, Map<String,String> tags, int eventDurationInMs) {
        if(connection.isConnected()) {
            eventQueue.add(builder.buildTimer(prefix, bucket, tags, eventDurationInMs));
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
