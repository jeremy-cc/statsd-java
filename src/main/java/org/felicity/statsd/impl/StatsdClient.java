package org.felicity.statsd.impl;

import org.felicity.statsd.StatsdClientInterface;
import org.felicity.statsd.impl.logging.SystemLogger;
import org.felicity.statsd.impl.transport.UdpConnectionInterface;

import java.util.AbstractQueue;
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

    public final String TIMED_EVENT_FORMAT = "%s.%s:%d|ms";
    public final String GAUGED_EVENT_FORMAT = "%s.%s:%d|g";
    public final String SIMPLE_COUNT_FORMAT = "%s.%s:%d|c";
    public final String SAMPLED_COUNT_FORMAT = "%s.%s:%d|@%.3f";

    // TCC-9027 do not enqueue new events if we do not have a connection established

    @Override
    public void incrementCounter(String prefix, String bucket, int count) {
        if(connection.isConnected()) {
            eventQueue.add(String.format(SIMPLE_COUNT_FORMAT, prefix, bucket, count));
        }
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, int count, double sampleRate) {
        if(connection.isConnected()) {
            eventQueue.add(String.format(SAMPLED_COUNT_FORMAT, prefix, bucket, count, sampleRate));
        }
    }

    @Override
    public void gaugeReading(String prefix, String bucket, int count) {
        if(connection.isConnected()) {
            eventQueue.add(String.format(GAUGED_EVENT_FORMAT, prefix, bucket, count));
        }
    }

    @Override
    public void timedEvent(String prefix, String bucket, int eventDurationInMs) {
        if(connection.isConnected()) {
            eventQueue.add(String.format(TIMED_EVENT_FORMAT, prefix, bucket, eventDurationInMs));
        }
    }

    @Override
    public void incrementUniqueCounter(String prefix, String bucket, int count) {
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
