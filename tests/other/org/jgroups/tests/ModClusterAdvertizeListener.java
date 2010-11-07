
package org.jgroups.tests;


import java.net.*;
import java.util.Enumeration;


/**
 Listens on 224.0.1.105:23364 for mod-cluster advertizements from httpd daemons and prints them to stdout
 @author Bela Ban
 @version $Revision: 1.1 $
 */
public class ModClusterAdvertizeListener {

    public static void main(String args[]) {
        MulticastSocket sock;
        InetAddress bind_addr=null, mcast_addr=null;
        DatagramPacket packet;
        byte buf[]=null;
        byte[] recv_buf;
        String tmp;
        int port=23364;
        boolean receive_on_all_interfaces=true;

        try {
            for(int i=0; i < args.length; i++) {
                tmp=args[i];
                if("-help".equals(tmp)) {
                    help();
                    return;
                }
                if("-bind_addr".equals(tmp)) {
                    bind_addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if("-mcast_addr".equals(tmp)) {
                    mcast_addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if("-port".equals(tmp)) {
                    port=Integer.parseInt(args[++i]);
                    continue;
                }
                if(("-receive_on_all_interfaces".equals(args[i]))) {
                    receive_on_all_interfaces=true;
                    continue;
                }
                help();
                return;
            }
            if(mcast_addr == null)
                mcast_addr=InetAddress.getByName("224.0.1.105");
        }
        catch(Exception ex) {
            System.err.println(ex);
            return;
        }


        try {
            sock=new MulticastSocket(port);
            SocketAddress join_addr=new InetSocketAddress(mcast_addr, port);


            if(receive_on_all_interfaces) {
                NetworkInterface intf;
                for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                    intf=(NetworkInterface)en.nextElement();
                    sock.joinGroup(join_addr, intf);
                    System.out.println("joined " + join_addr + " on " + intf.getName());
                }
            }
            else {
                if(bind_addr != null)
                    sock.setInterface(bind_addr);
                sock.joinGroup(join_addr, null);
            }


            System.out.println("Socket=" + sock.getLocalAddress() + ':' + sock.getLocalPort() + ", bind interface=" +
                               sock.getInterface());

            int length;
            while(true) {
                buf=new byte[256];
                packet=new DatagramPacket(buf, buf.length);
                sock.receive(packet);
                recv_buf=packet.getData();
                length=packet.getLength();
                System.out.println(new String(recv_buf, 0, length) + " [sender=" + packet.getAddress().getHostAddress() +
                        ':' + packet.getPort() + "]\n");
            }

        }
        catch(Exception e) {
            System.err.println(e);
        }

    }


    static void help() {
        System.out.println("McastSenderTest [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                           "[-port <port for multicast socket>] [-receive_on_all_interfaces]");
    }


}