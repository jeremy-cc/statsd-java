package org.felicity.statsd.impl.transport;

import org.felicity.statsd.impl.logging.SystemLogger;

import java.io.IOException;
import java.net.*;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the logic necessary to send messages to a remote UDP socket
 * Created by jeremyb on 03/04/2014.
 */
public class UdpConnection implements UdpConnectionInterface {
    final String CHARSET = "ISO-8859-1";
    final int MAX_COUNT = 3;

    private DatagramSocket socket = null;

    private String localHost;
    private String remoteHost;
    private int remotePort;
    private AtomicInteger connectionCount = new AtomicInteger(0);

    public UdpConnection(String host, int port) throws UnknownHostException {

        remoteHost = host;
        remotePort = port;
        localHost = InetAddress.getLocalHost().getCanonicalHostName().replaceAll("\\\\.", "_");
    }

    @Override
    public boolean isConnected() {
        return socket != null && socket.isConnected();
    }

    @Override
    public boolean send(String message) {
        try {
            sendMessage(message);
            return true;
        } catch (UnsupportedCharsetException uce) {
            SystemLogger.error(String.format("Unable to encode message in charset %s", CHARSET));
            return false;
        } catch (IOException ioe) {
            SystemLogger.error(String.format("Unable to send packet : %s", ioe.getMessage()));
            return false;
        }
    }

    public void sendMessage(String message) throws IOException {
        // prepend the local hostname
        String _msg = String.format("%s.%s", localHost, message);
        byte[] buffer = _msg.getBytes(CHARSET);
        getSocket().send(buildPacket(buffer));
    }

    private DatagramPacket buildPacket(byte[] buffer)  throws SocketException {
        DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
        pkt.setSocketAddress(getSocket().getRemoteSocketAddress());
        return pkt;
    }

    @Override
    public void connect() throws SocketException {
        disconnect(); // ensure we recycle any resources held by a prior connection
        attemptConnection();
    }

    private DatagramSocket getSocket() throws SocketException {
        attemptConnection();
        return socket;
    }

    private void attemptConnection() throws SocketException {
        int localCount = connectionCount.incrementAndGet();
        if(localCount > MAX_COUNT) {
            throw new SocketException("Exceeded max connection attempts; abandoning statsd connection");
        }
        if (null == socket) {
            socket = createSocket();
        }
        if (null != socket && !socket.isConnected()) {
            try {
                socket.connect(getRemoteEndpoint());
                connectionCount.compareAndSet(localCount, 0); // reset if nothing else has touched this.
            } catch (Exception exc) {
                SystemLogger.error(String.format("Unable to create socket : %s", exc.getMessage()));
            }
        }
    }

    public DatagramSocket createSocket() throws SocketException {
        return new DatagramSocket();
    }

    public InetSocketAddress getRemoteEndpoint() {
        return new InetSocketAddress(remoteHost, remotePort);
    }


    @Override
    public void disconnect() {
        if (isConnected()) {
            socket.disconnect();
            socket.close();
        }
    }

}
