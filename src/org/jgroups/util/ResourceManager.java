package org.jgroups.util;

import org.jgroups.Global;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.rmi.server.UID;
import java.util.StringTokenizer;

/**
 * Manages resources such as multicast addresses and multicast ports, and TCP ports
 * @author Bela Ban
 * @version $Id: ResourceManager.java,v 1.1 2008/04/10 10:25:08 belaban Exp $
 */
public class ResourceManager {
    private static final IpAddressRep rep;
    private static short mcast_port;


    static {
        String tmp_addr=System.getProperty(Global.INITIAL_MCAST_ADDR, "230.1.1.1");
        mcast_port=Short.valueOf(System.getProperty(Global.INITIAL_MCAST_PORT, "7000"));
        try {
            InetAddress tmp=InetAddress.getByName(tmp_addr);
            if(!tmp.isMulticastAddress())
                throw new IllegalArgumentException("initial multicast address " + tmp_addr +
                        " is not a valid multicast (class D) address");
            rep=new IpAddressRep(tmp_addr);
        }
        catch(UnknownHostException e) {
            throw new RuntimeException("initial multicast address " + tmp_addr + " is incorrect", e);
        }
    }

    private ResourceManager() {
    }

    /**
     * Returns the next available multicast address, e.g. "228.1.2.3". This class is a JVM singleton
     * @return
     */
    public static String getNextMulticastAddress() {
        return rep.nextAddress();
    }

    public static synchronized short getNextMulticastPort(InetAddress bind_addr) throws Exception {
        short port=mcast_port++;
        try {
            DatagramSocket sock=Util.createDatagramSocket(bind_addr, port);
            port=(short)sock.getLocalPort();
            sock.close();
            ServerSocket srv_sock=Util.createServerSocket(bind_addr, port);
            port=(short)srv_sock.getLocalPort();
            srv_sock.close();
            return port;
        }
        finally {
            mcast_port=(short)(port +1);
        }
    }


    public static String getUniqueClusterName(String base_name) {
        return base_name != null? base_name + "-" + new UID().toString() : new UID().toString();
    }

    public static String getUniqueClusterName() {
        return getUniqueClusterName(null);
    }

    public static void main(String[] args) throws Exception {
        for(int i=0; i < 10; i++) {
            String addr=getNextMulticastAddress();
            int mcast_port=getNextMulticastPort(InetAddress.getByName("192.168.1.5"));
            System.out.println("addr = " + addr + ":" + mcast_port);
        }

    }

    /** Representation of an IP address */
    static class IpAddressRep {
        short a=225, b=MIN, c=MIN, d=MIN;

        private static final short MIN=1, MAX=250;
        private static final char DOT='.';

        IpAddressRep(String initial_addr) {
            StringTokenizer tok=new StringTokenizer(initial_addr, ".", false);
            a=Short.valueOf(tok.nextToken());
            b=Short.valueOf(tok.nextToken());
            c=Short.valueOf(tok.nextToken());
            d=Short.valueOf(tok.nextToken());
        }

        synchronized String nextAddress() {
            StringBuilder sb=new StringBuilder();
            sb.append(a).append(DOT).append(b).append(DOT).append(c).append(DOT).append(d);
            increment();
            return sb.toString();
        }

        private void increment() {
            d++;
            if(d > MAX) {
                d=MIN;
                c++;
                if(c > MAX) {
                    c=MIN;
                    b++;
                    if(b > MAX) {
                        b=MIN;
                        a++;
                        if(a > MAX)
                            a=225;
                    }
                }
            }
        }
    }
}
