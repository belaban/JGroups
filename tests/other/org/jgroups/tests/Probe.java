package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
 * @version $Revision: 1.3 $
 * Date: Jun 2, 2003
 * Time: 4:35:29 PM
 */
public class Probe {
    MulticastSocket mcast_sock;
    DatagramSocket  sock;


    public Probe() {

    }

    public void start(InetAddress addr, int port, int ttl, final long timeout) throws Exception {
        mcast_sock=new MulticastSocket(port);
        mcast_sock.setTimeToLive(ttl);
        mcast_sock.joinGroup(addr);

        sock=new DatagramSocket();

        byte[] probe_buf=new byte[]{'d', 'i', 'a', 'g'};
        DatagramPacket probe=new DatagramPacket(probe_buf, 0, probe_buf.length, addr, port);
        sock.send(probe);
        System.out.println("\n-- send probe on " + addr + ":" + port + "\n");
        int i=0;

        new Thread() {
            public void run() {
                Util.sleep(timeout);
                sock.close();
            }
        }.start();

        for(;;) {
            byte[] buf=new byte[65000];
            DatagramPacket rsp=new DatagramPacket(buf, 0, buf.length);
            try {
                sock.receive(rsp);
            }
            catch(Throwable t) {
                System.out.println("\n");
                return;
            }
            byte[] data=rsp.getData();
            System.out.println("\n#" + ++i + ": " + new String(data, 0, rsp.getLength()));
        }
    }


    public static void main(String[] args) {
        InetAddress addr=null;
        int         port=0;
        int         ttl=32;
        long        timeout=10000;

        try {
            for(int i=0; i < args.length; i++) {
                if(args[i].equals("-addr")) {
                    addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if(args[i].equals("-port")) {
                    port=Integer.parseInt(args[++i]);
                    continue;
                }
                if(args[i].equals("-ttl")) {
                    ttl=Integer.parseInt(args[++i]);
                    continue;
                }
                if(args[i].equals("-timeout")) {
                    timeout=Long.parseLong(args[++i]);
                    continue;
                }

                help();
                return;
            }
            Probe p=new Probe();
            p.start(addr, port, ttl, timeout);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]");
    }
}
