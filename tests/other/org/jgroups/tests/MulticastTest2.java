package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;

/**
 * Tests 2 MulticastSockets joining the same multicast address and port (this should be possible).
 * This test fails when running as a native image under GraalVM 19.2.0.
 * @author Bela Ban
 * @since  4.1.5
 */
public class MulticastTest2 {

    protected static final InetAddress MCAST_ADDR, BIND_ADDR;
    protected static final int MCAST_PORT=7600;

    static {
        try {
            MCAST_ADDR=InetAddress.getByName("228.8.8.8");
            BIND_ADDR=InetAddress.getLocalHost();
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        MulticastSocket sock1=create();
        MulticastSocket sock2=create(); // this works under GraalVM 19.2.0
        System.out.printf("sock1: %s\nsock2: %s\n", sock1.getLocalSocketAddress(), sock2.getLocalSocketAddress());
        Util.close(sock1, sock2);

        sock1=new MulticastSocket(MCAST_PORT);
        sock2=new MulticastSocket(MCAST_PORT);  // this fails under GraalVM 19.2.0
    }



    protected static MulticastSocket create() throws IOException {
        MulticastSocket sock=null;
        SocketAddress saddr=new InetSocketAddress(MCAST_ADDR, MCAST_PORT);
        sock=new MulticastSocket(saddr);
        if(BIND_ADDR != null)
            sock.setNetworkInterface(NetworkInterface.getByInetAddress(BIND_ADDR));
        return sock;
    }

}
