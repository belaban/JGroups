package org.jgroups.tests;

import java.io.IOException;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;

/**
 * Tests {@link MulticastSocket#joinGroup(InetAddress)} and {@link DatagramChannel#join(InetAddress, NetworkInterface)}.
 * Both tests fail when running as a native image under GraalVM 19.0.1.
 * @author Bela Ban
 * @since  4.1.2
 */
public class MulticastTest {
    protected static final InetAddress group;
    protected static final InetAddress bind_addr;
    protected static final int         PORT=6789;

    static {
        try {
            group=InetAddress.getByName("239.5.5.5");
            bind_addr=InetAddress.getLocalHost();
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        MulticastTest test=new MulticastTest();
        if(args.length > 0 && args[0].equalsIgnoreCase("-nio"))
            test.testDatagramChannel();
        else
            test.testMulticastSocket();
    }


    protected void testMulticastSocket() throws IOException {
        MulticastSocket sock=new MulticastSocket(new InetSocketAddress(PORT));
        sock.setInterface(bind_addr);
        sock.joinGroup(group);
        System.out.printf("%s: local=%s, remote=%s\n", sock.getClass().getSimpleName(),
                          sock.getLocalSocketAddress(), sock.getRemoteSocketAddress());
    }

    protected void testDatagramChannel() throws IOException {
        NetworkInterface ni = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

        DatagramChannel dc = DatagramChannel.open(StandardProtocolFamily.INET)
          .setOption(StandardSocketOptions.SO_REUSEADDR, true)
          .bind(new InetSocketAddress(PORT))
          .setOption(StandardSocketOptions.IP_MULTICAST_IF, ni);

        MembershipKey key = dc.join(group, ni);
        System.out.println("key = " + key);
    }

}
