package com.currencycloud.statsd.impl.transport;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import static org.mockito.Mockito.*;

/**
 * Created by jeremyb on 08/04/2014.
 */
@RunWith(MockitoJUnitRunner.class)
public class UdpConnectionTest {

    @Mock
    private DatagramSocket mockSocket;
    @Mock
    private InetSocketAddress socketAddress;

    @Mock
    private Object mockPacket;

    private UdpConnection subject;
    private UdpConnection spy_subject;

    @Before
    public void init() throws IOException {
        subject = new UdpConnection("localhost", 8080);

        spy_subject = spy(subject);

        doReturn(mockSocket).when(spy_subject).createSocket();
        doNothing().when(spy_subject).sendMessage(anyString());
        doReturn(true).when(mockSocket).isConnected();
        doReturn(socketAddress).when(spy_subject).getRemoteEndpoint();
        doReturn(mockSocket.isConnected()).when(spy_subject).isConnected();
    }

    @Test
    public void testIsConnected() throws Exception {
        spy_subject.isConnected();

        verify(mockSocket).isConnected();
    }

    @Test
    public void testSend() throws Exception {
        spy_subject.send("abc");

        verify(spy_subject).sendMessage("abc");
    }

    @Test
    public void testConnect() throws Exception {
        reset(mockSocket);
        doReturn(false).when(spy_subject).isConnected();

        spy_subject.connect();

        verify(mockSocket).connect(Matchers.<java.net.SocketAddress>any());
    }

    @Test
    public void testDisconnect() throws Exception {

    }
}
