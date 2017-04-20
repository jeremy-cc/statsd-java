package com.ccycloud.aws.statsd;

import com.ccycloud.aws.statsd.impl.config.Configuration;
import com.ccycloud.aws.statsd.impl.config.MissingConfigurationException;
import com.ccycloud.aws.statsd.impl.logging.SystemLogger;
import com.ccycloud.aws.statsd.impl.transport.UdpConnection;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for the statsd package.  Supports configuration; retrieval of the actual messaging interface
 * and any and sundry functions
 * <p/>
 * Created by jeremyb on 03/04/2014.
 */
public class StatsdClient implements StatsdClientInterface {
    private static volatile StatsdClient instance = null;
    private Map<String, String> configuration = null;

    private UdpConnection connection = null;

    private com.ccycloud.aws.statsd.impl.StatsdClient client = null;

    private AtomicBoolean configured = new AtomicBoolean(false);
    private AtomicBoolean connected = new AtomicBoolean(false);

    public static final String version = "1.0.15";

    /**
     * Get an instance of the statsd object, configured with the passed config hash
     *
     * @param configuration a Map of string key / value pairs configuring the instance
     * @throws com.ccycloud.aws.statsd.impl.config.MissingConfigurationException if mandatory keys are not provided
     */
    public static StatsdClient get(Map<String, String> configuration) throws MissingConfigurationException, UnknownHostException {
        if(null == instance) {
            synchronized (com.ccycloud.aws.statsd.impl.StatsdClient.class) {
                if (null == instance) {
                    instance = new StatsdClient();
                    SystemLogger.info(String.format("Configuring connection to statsd server; client version: %s", version));
                    instance.configureWith(instance.validateMandatoryConfiguration(configuration));
                    instance.restart();
                }
            }
        }
        return instance;
    }

    @Override
    public void incrementCounter(String prefix, String bucket, Map<String,String> tags, int count) {
        client.incrementCounter(prefix, bucket, tags, count);
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, Map<String,String> tags, int count, double sampleRate) {
        client.incrementSampleCounter(prefix, bucket, tags, count, sampleRate);
    }

    @Override
    public void gaugeReading(String prefix, String bucket, Map<String,String> tags, int count) {
        client.gaugeReading(prefix, bucket, tags, count);
    }

    @Override
    public void timedEvent(String prefix, String bucket, Map<String,String> tags, int eventDurationInMs) {
        client.timedEvent(prefix, bucket, tags, eventDurationInMs);
    }

    @Override
    public void incrementUniqueCounter(String prefix, String bucket, Map<String,String> tags, int count) {
        client.incrementUniqueCounter(prefix, bucket, tags, count);
    }

    public boolean isConfigured() {
        return configured.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    public com.ccycloud.aws.statsd.impl.StatsdClient buildClient(UdpConnection connection) {
        return new com.ccycloud.aws.statsd.impl.StatsdClient(connection);
    }

    public void disconnect() {
        if(null != connection && connection.isConnected()) {
            if(null != client) {
                // ensure we spool any queued data
                client.finishMeasurements();
            }
            connection.disconnect();
        }
    }

    /**
     * close and recycle any UDP connections underlying this statsd object, create a new connection, and bind to the remote host
     */
    private void restart() throws UnknownHostException{
        if(null != connection && connection.isConnected()) {
            if(null != client) {
                // ensure we spool any queued data
                client.finishMeasurements();
            }
            connection.disconnect();
            connected.compareAndSet(true, false);
        }

        String host = configuration.get(Configuration.CONFIG_HOST);
        int port = Integer.parseInt(configuration.get(Configuration.CONFIG_PORT));
        connection = new UdpConnection(host, port);
        client = buildClient(connection);

        try {
            connection.connect();
            if(connection.isConnected()) {
                connected.compareAndSet(false, true);
                client.startMeasurements();
            }
        } catch(SocketException se) {
            SystemLogger.error("Unable to connect to remote host", se);
        }
    }

    // private methods
    private void configureWith(Map<String, String> configuration) {
        this.configuration = configuration;
        this.configured.compareAndSet(false, true);
    }

    private Map<String, String> validateMandatoryConfiguration(Map<String, String> proposedConfiguration) throws MissingConfigurationException {
        for (String s : Configuration.getMandatoryConfigKeys()) {
            if (!proposedConfiguration.containsKey(s)) {
                throw new MissingConfigurationException("Error: mandatory configuration value " + s + " was not specified.");
            }
        }
        return proposedConfiguration;
    }

    private StatsdClient() {
        super();
    }
}
