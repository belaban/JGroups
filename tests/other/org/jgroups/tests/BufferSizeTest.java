package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.MulticastSocket;
import java.net.SocketException;

/**
 * Tests max datagram and multicast socket buffer sizes
 * @author Bela Ban
 * @since  5.3.5
 */
public class BufferSizeTest {

    protected static void start(int size) throws IOException {
        MulticastSocket mcast_sock=new MulticastSocket();
        setBufferSize(mcast_sock, size, true);
        setBufferSize(mcast_sock, size, false);

        DatagramSocket sock=new DatagramSocket();
        setBufferSize(sock, size, true);
        setBufferSize(sock, size, false);
    }

    protected static void setBufferSize(DatagramSocket sock, int size, boolean recv_buf) throws SocketException {
        int prev_size=recv_buf? sock.getReceiveBufferSize() : sock.getSendBufferSize();
        if(recv_buf)
            sock.setReceiveBufferSize(size);
        else
            sock.setSendBufferSize(size);
        int new_size=recv_buf? sock.getReceiveBufferSize() : sock.getSendBufferSize();
        if(new_size == size) {
            System.out.printf("-- %s OK: set %s buffer from %s to %s\n",
                              sock.getClass().getSimpleName(), recv_buf? "recv" : "send", Util.printBytes(prev_size),
                              Util.printBytes(size));
        }
        else {
            System.err.printf("-- %s FAIL: set %s buffer from %s to %s (actual size: %s)\n",
                              sock.getClass().getSimpleName(), recv_buf? "recv" : "send", Util.printBytes(prev_size),
                              Util.printBytes(size), Util.printBytes(new_size));
        }
    }

    public static void main(String[] args) throws IOException {
        int size=50_000_000;
        for(int i=0; i < args.length; i++) {
            if("-size".equals(args[i])) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }
        BufferSizeTest.start(size);
    }

    protected static void help() {
        System.out.printf("%s [-size <buffer size in bytes>]\n", BufferSizeTest.class.getSimpleName());
    }
}
