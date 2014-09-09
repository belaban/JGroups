
package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.List;


/**
 * Tests IP multicast.
 */
public class mcast {
    protected DatagramSocket  sock;       // for sending of multicasts
    protected InetAddress     mcast_addr=null, bind_addr=null;
    protected int             mcast_port=5555;


    public mcast(InetAddress bind_addr, InetAddress mcast_addr, int mcast_port) {
        this.bind_addr=bind_addr;
        this.mcast_addr=mcast_addr;
        this.mcast_port=mcast_port;
    }

    protected void start() {
        try {
            Receiver r=new Receiver();
            r.start();

            /*DatagramChannel channel=DatagramChannel.open();
            channel.bind(new InetSocketAddress(bind_addr, 0));
            channel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, 8);
            sock=channel.socket();*/

            sock=new DatagramSocket(0, bind_addr);
        }
        catch(Exception e) {
            System.err.println(e);
        }

        DataInputStream in=new DataInputStream(System.in);
        while(true) {
            System.out.print("> ");
            try {
                String line=Util.readLine(in);
                byte[] buf=line.getBytes();
                DatagramPacket packet=new DatagramPacket(buf, buf.length, mcast_addr, mcast_port);
                sock.send(packet);
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        InetAddress mcast_addr=null, bind_addr=null;
        int mcast_port=5555;

        try {
            for(int i=0; i < args.length; i++) {
                String tmp=args[i];
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
                if("-mcast_port".equals(tmp)) {
                    mcast_port=Integer.parseInt(args[++i]);
                    continue;
                }

                help();
                return;
            }
            if(mcast_addr == null)
                mcast_addr=InetAddress.getByName("232.5.5.5");
        }
        catch(Exception ex) {
            System.err.println(ex);
            return;
        }

        mcast mcast=new mcast(bind_addr, mcast_addr, mcast_port);
        mcast.start();
    }



    static void help() {
        System.out.println("mcast [-bind_addr <bind address>] [-help] [-mcast_addr <multicast address>] " +
                             "[-port <port for multicast socket>]\n" +
                             "(Note that a null bind_addr will join the receiver multicast socket on all interfaces)");
    }

    protected void bindToInterfaces(List<NetworkInterface> interfaces, MulticastSocket s) {
        SocketAddress tmp_mcast_addr=new InetSocketAddress(mcast_addr, mcast_port);
        for(NetworkInterface intf: interfaces) {
            try {
                s.joinGroup(tmp_mcast_addr, intf);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
    }


    protected class Receiver extends Thread {
        protected MulticastSocket mcast_sock;

        protected Receiver() throws Exception {
            InetSocketAddress saddr=new InetSocketAddress(mcast_addr, mcast_port);
            mcast_sock=new MulticastSocket(saddr);
            // if(bind_addr != null)
               // mcast_sock.setInterface(bind_addr);

            if(bind_addr != null)
                bindToInterfaces(Arrays.asList(NetworkInterface.getByInetAddress(bind_addr)), mcast_sock);
            else {
                List<NetworkInterface> intf_list=Util.getAllAvailableInterfaces();
                System.out.println("Joining " + saddr + " on interfaces: " + intf_list);
                bindToInterfaces(intf_list, mcast_sock);
            }

            System.out.println("Socket=" + mcast_sock.getLocalAddress() + ':' + mcast_sock.getLocalPort() + ", bind interface=" +
                                 mcast_sock.getInterface());
        }


        public void run() {
            while(true) {
                try {
                    byte[] buf=new byte[256];
                    DatagramPacket packet=new DatagramPacket(buf, buf.length);
                    mcast_sock.receive(packet);
                    byte[] recv_buf=packet.getData();
                    int recv_len=packet.getLength();
                    System.out.println(new String(recv_buf,0,recv_len) + " [sender=" + packet.getAddress().getHostAddress() +
                            ':' + packet.getPort() + ']');
                }
                catch(Exception ex) {
                    System.err.println("Receiver terminated: " + ex);
                    break;
                }
            }
        }
    }


}
