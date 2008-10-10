package org.jgroups.tests;

import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.*;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
 * @version $Revision: 1.16 $
 * Date: Jun 2, 2003
 * Time: 4:35:29 PM
 */
public class Probe {
    MulticastSocket mcast_sock;
    volatile boolean running=true;
    final Set<String> senders=new HashSet<String>();


    public Probe() {

    }

    public void start(InetAddress addr, InetAddress bind_addr, int port, int ttl,
                      final long timeout, List query, String match, boolean weed_out_duplicates) throws Exception {
        mcast_sock=new MulticastSocket();
        mcast_sock.setTimeToLive(ttl);
        if(bind_addr != null)
            mcast_sock.setInterface(bind_addr);

        StringBuilder request=new StringBuilder();
        for(int i=0; i < query.size(); i++) {
            request.append(query.get(i)).append(" ");
        }
        byte[] probe_buf=request.toString().getBytes();

        DatagramPacket probe=new DatagramPacket(probe_buf, 0, probe_buf.length, addr, port);
        mcast_sock.send(probe);
        System.out.println("\n-- send probe on " + addr + ':' + port + '\n');


        new Thread() {
            public void run() {
                Util.sleep(timeout);
                mcast_sock.close();
                running=false;
            }
        }.start();

        int matched=0, not_matched=0, count=0;
        String response;
        while(running) {
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
            response=new String(data, 0, rsp.getLength());
            if(weed_out_duplicates && checkDuplicateResponse(response)) {
                continue;
            }

            if(matches(response, match)) {
                matched++;
                System.out.println("\n#" + ++count + " (" + rsp.getLength() + " bytes):\n" + response);
            }
            else
                not_matched++;
        }
        System.out.println("\nTotal responses=" + count + ", " + matched + " matches, " + not_matched + " non-matches");
    }

    private boolean checkDuplicateResponse(String response) {
        int index=response.indexOf("local_addr");
        if(index != -1) {
            String addr=parseAddress(response.substring(index+1 + "local_addr".length()));
            return senders.add(addr) == false;
        }

        return false;
    }

    private static String parseAddress(String response) {
        StringTokenizer st=new StringTokenizer(response);
        return st.nextToken();
    }

    private static boolean matches(String response, String match) {
        if(response == null)
            return false;
        if(match == null)
            return true;
        int index=response.indexOf(match);
        return index > -1;
    }


    public static void main(String[] args) {
        InetAddress  addr=null, bind_addr=null;
        int          port=0;
        int          ttl=32;
        long         timeout=2000;
        final String DEFAULT_DIAG_ADDR="224.0.75.75";
        final int    DEFAULT_DIAG_PORT=7500;
        List<String> query=new ArrayList<String>();
        String       match=null;
        boolean      weed_out_duplicates=false;

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
                if("-match".equals(args[i])) {
                    match=args[++i];
                    continue;
                }
                if("-weed_out_duplicates".equals(args[i])) {
                    weed_out_duplicates=true;
                    continue;
                }
                if("-help".equals(args[i]) || "-h".equals(args[i])) {
                    help();
                    return;
                }
                query.add(args[i]);
            }
            Probe p=new Probe();
            if(addr == null)
                addr=InetAddress.getByName(DEFAULT_DIAG_ADDR);
            if(port == 0)
                port=DEFAULT_DIAG_PORT;
            p.start(addr, bind_addr, port, ttl, timeout, query, match, weed_out_duplicates);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-bind_addr <addr>] " +
                "[-port <port>] [-ttl <ttl>] [-timeout <timeout>] [-weed_out_duplicates] " +
                "[-match <pattern>] QUERY\n" +
                "(QUERY is a whitespace separate list of keys)");
    }
}
