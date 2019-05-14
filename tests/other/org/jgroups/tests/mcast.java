
package org.jgroups.tests;

import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.List;


/**
 * Tests IP multicast.
 */
public class mcast {
    protected DatagramSocket  sock;       // for sending of multicasts
    protected InetAddress     mcast_addr=null, bind_addr=null;
    protected int             mcast_port=5555;
    protected final int       local_port;
    protected final int       ttl;
    protected static final boolean        can_bind_to_mcast_addr;
    protected static final StackType      ip_version;
    protected static final ProtocolFamily prot_family;

    static {
        can_bind_to_mcast_addr=(Util.checkForLinux() && !Util.checkForAndroid())
          || Util.checkForSolaris()
          || Util.checkForHp()
          || Util.checkForMac();
        ip_version=Util.getIpStackType();
        prot_family=ip_version == StackType.IPv6? StandardProtocolFamily.INET6 : StandardProtocolFamily.INET;
    }


    public mcast(InetAddress bind_addr, int local_port, InetAddress mcast_addr, int mcast_port, int ttl) {
        this.bind_addr=bind_addr;
        this.local_port=local_port;
        this.mcast_addr=mcast_addr;
        this.mcast_port=mcast_port;
        this.ttl=ttl;
    }

    protected void start() throws Exception {
        Receiver r=null;
        DatagramChannel channel=null;
        try {
            r=new Receiver();
            r.start();

            channel=DatagramChannel.open(prot_family).setOption(StandardSocketOptions.IP_MULTICAST_TTL, ttl)
              .bind(new InetSocketAddress(bind_addr, local_port));
            sock=channel.socket();
        }
        catch(Exception ex) {
            if(channel != null)
                channel.close();
            if(r != null) {
                r.kill();
                r.interrupt();
            }
            throw ex;
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

    public static void main(String[] args) throws Exception {
        InetAddress mcast_addr=null, bind_addr=null;
        int mcast_port=5555;
        int local_port=0;
        int ttl=8;

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
                if("-port".equals(tmp)) {
                    local_port=Integer.parseInt(args[++i]);
                    continue;
                }
                if("-ttl".equals(tmp)) {
                    ttl=Integer.parseInt(args[++i]);
                    continue;
                }
                help();
                return;
            }
            if(mcast_addr == null) {
                switch(ip_version) {
                    case IPv4:
                        mcast_addr=InetAddress.getByName("232.5.5.5");
                        break;
                    case IPv6:
                        mcast_addr=InetAddress.getByName("ff0e::8:8:8");
                        break;
                }
            }
        }
        catch(Exception ex) {
            System.err.println(ex);
            return;
        }

        mcast mcast=new mcast(bind_addr, local_port, mcast_addr, mcast_port, ttl);
        mcast.start();
    }



    static void help() {
        System.out.println("mcast [-help]\n" +
                             "      [-bind_addr <bind address>]\n" +
                             "      [-port <local port>]\n" +
                             "      [-mcast_addr <multicast address>]\n" +
                             "      [-mcast_port <port for multicast socket>]\n" +
                             "      [-ttl <TTL>]\n" +
                             "(Note that a null bind_addr will join the receiver multicast socket on all interfaces)\n");
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

            if(can_bind_to_mcast_addr)
                mcast_sock=new MulticastSocket(saddr);
            else
                mcast_sock=new MulticastSocket(mcast_port);

            if(bind_addr != null)
                bindToInterfaces(Collections.singletonList(NetworkInterface.getByInetAddress(bind_addr)), mcast_sock);
            else {
                List<NetworkInterface> intf_list=Util.getAllAvailableInterfaces();
                System.out.println("Joining " + saddr + " on interfaces: " + intf_list);
                bindToInterfaces(intf_list, mcast_sock);
            }

            System.out.println("Socket=" + mcast_sock.getLocalAddress() + ':' + mcast_sock.getLocalPort() + ", bind interface=" +
                                 mcast_sock.getInterface());
        }

        protected void kill() {
            mcast_sock.close();
        }


        public void run() {
            while(true) {
                try {
                    byte[] buf=new byte[1024];
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
