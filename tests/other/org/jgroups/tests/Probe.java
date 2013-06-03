package org.jgroups.tests;

import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.net.*;
import java.util.*;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
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
                      final long timeout, List query, String match, boolean weed_out_duplicates, String passcode) throws Exception {

        boolean unicast_dest=addr != null && !addr.isMulticastAddress();
        if(unicast_dest) {
            Collection<InetAddress> targets=getPhysicalAddresses(addr, bind_addr, port, timeout);
            if(targets.isEmpty()) {
                System.err.println("Found no valid hosts - terminating");
                return;
            }
            for(InetAddress target: targets) {
                sendRequest(target, bind_addr, port, ttl, query, passcode);
            }
        }
        else
            sendRequest(addr, bind_addr, port, ttl, query, passcode);

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
            byte[] buf=new byte[70000];
            DatagramPacket rsp=new DatagramPacket(buf, 0, buf.length);
            try {
                mcast_sock.receive(rsp);
            }
            catch(Throwable t) {
                System.out.println("\n");
                break;
            }

            byte[] data=rsp.getData();
            response=new String(data, 0, rsp.getLength());
            if(weed_out_duplicates && checkDuplicateResponse(response))
                continue;

            count++;
            if(matches(response, match)) {
                matched++;
                System.out.println("\n#" + count + " (" + rsp.getLength() + " bytes):\n" + response);
            }
            else
                not_matched++;
        }
        System.out.println("\n" + count + " responses (" + matched + " matches, " + not_matched + " non matches)");
    }

    protected static Collection<InetAddress> getPhysicalAddresses(InetAddress addr, InetAddress bind_addr,
                                                                  int port, final long timeout) throws Exception {
        final DatagramSocket sock=new DatagramSocket(new InetSocketAddress(bind_addr, 0));
        byte[] payload="addrs".getBytes();
        DatagramPacket probe=new DatagramPacket(payload, 0, payload.length, addr, port);
        sock.send(probe);

        new Thread() {
            public void run() {
                Util.sleep(timeout);
                sock.close();
            }
        }.start();

        Collection<InetAddress> retval=new ArrayList<InetAddress>();
        long end_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < end_time) {
            byte[] buf=new byte[70000];
            DatagramPacket rsp=new DatagramPacket(buf, 0, buf.length);
            try {
                sock.receive(rsp);
            }
            catch(Throwable t) {
                break;
            }

            byte[] data=rsp.getData();
            String response=new String(data, 0, rsp.getLength());
            if(response != null && response.startsWith("addrs=")) {
                response=response.substring("addrs=".length()).trim();
                List<String> rsps=Util.parseStringList(response,",");
                for(String tmp: rsps) {
                    int index=tmp.indexOf(':');
                    if(index != -1)
                        tmp=tmp.substring(0, index);
                    retval.add(InetAddress.getByName(tmp));
                }
                return retval;
            }
        }
        return retval;
    }


    protected void sendRequest(InetAddress addr, InetAddress bind_addr, int port, int ttl,
                               List query, String passcode) throws Exception {
        if(mcast_sock == null) {
            mcast_sock=new MulticastSocket();
            mcast_sock.setTimeToLive(ttl);
            if(bind_addr != null)
                mcast_sock.setInterface(bind_addr);
        }

        StringBuilder request=new StringBuilder();
        byte[] authenticationDigest = null;
        if(passcode != null){
            long t1 = (new Date()).getTime();
            double q1 = Math.random();
            authenticationDigest = Util.createAuthenticationDigest(passcode, t1, q1);
        }
        for(int i=0; i < query.size(); i++) {
            request.append(query.get(i)).append(" ");
        }
        byte[] queryPayload = request.toString().getBytes();
        byte[] payload = queryPayload;
        if (authenticationDigest != null) {
            payload = new byte[authenticationDigest.length + queryPayload.length];
            System.arraycopy(authenticationDigest, 0, payload, 0, authenticationDigest.length);
            System.arraycopy(queryPayload, 0, payload, authenticationDigest.length, queryPayload.length);
        }

        DatagramPacket probe=new DatagramPacket(payload, 0, payload.length, addr, port);
        mcast_sock.send(probe);
        System.out.println("\n-- sending probe on " + addr + ':' + port + '\n');
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
        long         timeout=500;
        final String DEFAULT_DIAG_ADDR="224.0.75.75";
        final String DEFAULT_DIAG_ADDR_IPv6="ff0e::0:75:75";
        final int    DEFAULT_DIAG_PORT=7500;
        List<String> query=new ArrayList<String>();
        String       match=null;
        boolean      weed_out_duplicates=false;
        String       passcode=null;

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
                if("-passcode".equals(args[i])) {
                   passcode=args[++i];
                   continue;
               }
                if("-help".equals(args[i]) || "-h".equals(args[i]) || "--help".equals(args[i])) {
                    help();
                    return;
                }
                query.add(args[i]);
            }
            Probe p=new Probe();
            if(addr == null) {
                StackType stack_type=Util.getIpStackType();
                boolean ipv6=stack_type == StackType.IPv6;
                addr=ipv6? InetAddress.getByName(DEFAULT_DIAG_ADDR_IPv6) : InetAddress.getByName(DEFAULT_DIAG_ADDR);
            }
            if(port == 0)
                port=DEFAULT_DIAG_PORT;
            p.start(addr, bind_addr, port, ttl, timeout, query, match, weed_out_duplicates, passcode);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-bind_addr <addr>] " +
                             "[-port <port>] [-ttl <ttl>] [-timeout <timeout>] [-passcode <code>] [-weed_out_duplicates] " +
                             "[-match pattern] " +
                             "[key[=value]]*\n\n" +
                             "Examples:\n" +
                             "probe.sh keys // dumps all valid commands\n" +
                             "probe.sh jmx=NAKACK // dumps JMX info about all NAKACK protocols\n" +
                             "probe.sh op=STABLE.runMessageGarbageCollection // invokes the method in all STABLE protocols\n" +
                             "probe.sh jmx=UDP.oob,thread_pool // dumps all attrs of UDP starting with oob* or thread_pool*\n" +
                             "probe.sh jmx=FLUSH.bypass=true\n");
    }
}
