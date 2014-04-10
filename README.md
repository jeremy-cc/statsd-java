statsd-java
===========

# Version 1.0

A lightweight, concurrent, threaded Statsd client for use in Java or JRuby applications which require only basic instrumentation.

# Description

statsd-java is intended to be a lightweight statsd-compliant client which is able to send statsd instrumentation messages via UDP to a statsd server. It makes use of a single internal thread and a Conccurrent Queue in order to buffer
data for dispatch asynchronously to the dispatch of this data.

Usage:

        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("statsd.host", "localhost");
        configuration.put("statsd.port", "8080");

        // create and obtain an instance
        Statsd instance = Statsd.getInstance(configuration);
        
        // increment a counter
        instance.incrementCounter("pricing_engine", "test_counter", 1);
        
        // disconnect, cleanly close the thread and ensure any unsent buffered data is sent
        instance.disconnect();
        
# 