package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
 * @version $Revision: 1.8 $
 * Date: Jun 2, 2003
 * Time: 4:35:29 PM
 */
public class Probe {
    MulticastSocket mcast_sock;


    public Probe() {

    }

    public void start(InetAddress addr, InetAddress bind_addr, int port, int ttl, final long timeout) throws Exception {
        mcast_sock=new MulticastSocket();
        mcast_sock.setTimeToLive(ttl);
        if(bind_addr != null)
            mcast_sock.setInterface(bind_addr);

        byte[] probe_buf=new byte[]{'d', 'i', 'a', 'g'};
        DatagramPacket probe=new DatagramPacket(probe_buf, 0, probe_buf.length, addr, port);
        mcast_sock.send(probe);
        System.out.println("\n-- send probe on " + addr + ':' + port + '\n');
        int i=0;

        new Thread() {
            public void run() {
                Util.sleep(timeout);
                mcast_sock.close();
            }
        }.start();

        for(;;) {
            byte[] buf=new byte[65000];
            DatagramPacket rsp=new DatagramPacket(buf, 0, buf.length);
            try {
                mcast_sock.receive(rsp);
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
        InetAddress addr=null, bind_addr=null;
        int         port=0;
        int         ttl=32;
        long        timeout=10000;

        try {
            for(int i=0; i < args.length; i++) {
                if("-addr".equals(args[i])) {
                    addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if("-bind_addr".equals(args[i])) {
                    bind_addr=InetAddress.getByName(args[++i]);
                    continue;
                }
                if("-port".equals(args[i])) {
                    port=Integer.parseInt(args[++i]);
                    continue;
                }
                if("-ttl".equals(args[i])) {
                    ttl=Integer.parseInt(args[++i]);
                    continue;
                }
                if("-timeout".equals(args[i])) {
                    timeout=Long.parseLong(args[++i]);
                    continue;
                }

                help();
                return;
            }
            Probe p=new Probe();
            p.start(addr, bind_addr, port, ttl, timeout);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-bind_addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]");
    }
}
