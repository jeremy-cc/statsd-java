package com.ccycloud.aws.statsd.impl.transport;

import java.net.SocketException;

/**
 * Created by jeremyb on 08/04/2014.
 */
public interface UdpConnectionInterface {
    boolean isConnected();

    boolean send(String message);

    void connect() throws SocketException;

    void disconnect();
}
