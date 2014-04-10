package org.felicity.statsd;

import org.felicity.statsd.impl.config.Configuration;
import org.felicity.statsd.impl.config.MissingConfigurationException;
import org.felicity.statsd.impl.logging.SystemLogger;
import org.felicity.statsd.impl.transport.UdpConnection;

import java.net.SocketException;
import java.util.Map;

/**
 * Base class for the statsd package.  Supports configuration; retrieval of the actual messaging interface
 * and any and sundry functions
 * <p/>
 * Created by jeremyb on 03/04/2014.
 */
public class StatsdClient implements StatsdClientInterface {
    private static StatsdClient instance = new StatsdClient();
    private Map<String, String> configuration = null;

    private UdpConnection connection = null;

    private org.felicity.statsd.impl.StatsdClient client = null;

    /**
     * Get an instance of the statsd object, configured with the passed config hash
     *
     * @param configuration a Map of string key / value pairs configuring the instance
     * @throws org.felicity.statsd.impl.config.MissingConfigurationException if mandatory keys are not provided
     */
    public static StatsdClient getInstance(Map<String, String> configuration) throws MissingConfigurationException {
        SystemLogger.info("Configuring connection to statsd server");
        instance.configureWith(instance.validateMandatoryConfiguration(configuration));
        instance.restart();
        return instance;
    }

    @Override
    public void incrementCounter(String prefix, String bucket, int count) {
        client.incrementCounter(prefix, bucket, count);
    }

    @Override
    public void incrementSampleCounter(String prefix, String bucket, int count, double sampleRate) {
        client.incrementSampleCounter(prefix, bucket, count, sampleRate);
    }

    @Override
    public void gaugeReading(String prefix, String bucket, int count) {
        client.gaugeReading(prefix, bucket, count);
    }

    @Override
    public void timedEvent(String prefix, String bucket, int eventDurationInMs) {
        client.timedEvent(prefix, bucket, eventDurationInMs);
    }

    @Override
    public void incrementUniqueCounter(String prefix, String bucket, int count) {
        client.incrementUniqueCounter(prefix, bucket, count);
    }

    public org.felicity.statsd.impl.StatsdClient buildClient(UdpConnection connection) {
        return new org.felicity.statsd.impl.StatsdClient(connection);
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
    private void restart() {
        if(null != connection && connection.isConnected()) {
            if(null != client) {
                // ensure we spool any queued data
                client.finishMeasurements();
            }
            connection.disconnect();
        }
        String host = configuration.get(Configuration.CONFIG_HOST);
        int port = Integer.parseInt(configuration.get(Configuration.CONFIG_PORT));
        connection = new UdpConnection(host, port);
        client = buildClient(connection);
        try {
            connection.connect();
            client.startMeasurements();
        } catch(SocketException se) {
            SystemLogger.error("Unable to connect to remote host", se);
        }
    }

    // private methods
    private void configureWith(Map<String, String> configuration) {
        this.configuration = configuration;
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
