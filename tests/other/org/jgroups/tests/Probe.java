package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.ArrayList;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
 * @version $Revision: 1.9 $
 * Date: Jun 2, 2003
 * Time: 4:35:29 PM
 */
public class Probe {
    MulticastSocket mcast_sock;


    public Probe() {

    }

    public void start(InetAddress addr, InetAddress bind_addr, int port, int ttl,
                      final long timeout, List query) throws Exception {
        mcast_sock=new MulticastSocket();
        mcast_sock.setTimeToLive(ttl);
        if(bind_addr != null)
            mcast_sock.setInterface(bind_addr);

        StringBuffer request=new StringBuffer("QUERY: ");
        for(int i=0; i < query.size(); i++) {
            request.append(query.get(i)).append(" ");
        }
        byte[] probe_buf=request.toString().getBytes();

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
            // System.out.println("received " + rsp.getLength() + " bytes from " + rsp.getSocketAddress());
            System.out.println("\n#" + ++i + " (" + rsp.getLength() + " bytes): " + new String(data, 0, rsp.getLength()));
        }
    }


    public static void main(String[] args) {
        InetAddress  addr=null, bind_addr=null;
        int          port=0;
        int          ttl=32;
        long         timeout=10000;
        final String DEFAULT_DIAG_ADDR="224.0.0.75";
        final int    DEFAULT_DIAG_PORT=7500;
        List         query=new ArrayList();

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
                if("-query".equals(args[i])) {
                    query.add(args[++i]);
                    continue;
                }

                help();
                return;
            }
            Probe p=new Probe();
            if(addr == null)
                addr=InetAddress.getByName(DEFAULT_DIAG_ADDR);
            if(port == 0)
                port=DEFAULT_DIAG_PORT;
            p.start(addr, bind_addr, port, ttl, timeout, query);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-bind_addr <addr>] " +
                "[-port <port>] [-ttl <ttl>] [-timeout <timeout>] [-query <query>]" +
                " (query can be jmx or props)");
    }
}
