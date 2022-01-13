package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ByteArray;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Discovers all UDP-based members running on a certain mcast address
 * @author Bela Ban
 */
public class Probe {
    protected final Set<String>     senders=new HashSet<>();
    protected boolean               weed_out_duplicates;
    protected String                match;
    protected static final int      DEFAULT_DIAG_PORT=7500;
    protected static final String   MEMBER_ADDRS="member-addrs";
    protected ExecutorService       thread_pool;
    protected final List<Requester> requesters=new ArrayList<>();
    protected final AtomicInteger   matched=new AtomicInteger(), not_matched=new AtomicInteger(), count=new AtomicInteger();
    protected boolean               verbose;
    protected static final String   PREFIX="**";


    public boolean verbose()          {return verbose;}
    public Probe   verbose(boolean b) {verbose=b; return this;}


    public void start(List<InetAddress> addrs, InetAddress bind_addr, int port, int ttl,
                      final long timeout, String request, String match,
                      boolean weed_out_duplicates, String passcode, boolean udp, boolean tcp) throws Exception {
        this.weed_out_duplicates=weed_out_duplicates;
        this.match=match;
        thread_pool=Executors.newCachedThreadPool(new DefaultThreadFactory("probe", true, true));

        if(verbose)
            System.out.printf("%s addrs: %s\n%s udp: %b, tcp: %b\n\n", PREFIX, addrs, PREFIX, udp, tcp);

        for(InetAddress addr: addrs) {
            boolean unicast_dest=addr != null && !addr.isMulticastAddress();
            if(unicast_dest)
                fetchAddressesAndInvoke(new InetSocketAddress(addr, port), bind_addr, request, passcode, timeout, ttl, udp, tcp);
            else { // multicast - requires a UdpRequester
                Requester req=new UdpRequester(new InetSocketAddress(addr, port), request, passcode, null)
                  .start(bind_addr, timeout, ttl);
                requesters.add(req);
                thread_pool.execute(req);
            }
        }
        Util.sleep(timeout);
        requesters.forEach(Requester::stop);
        thread_pool.shutdown();
        thread_pool.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        System.out.printf("%d responses (%d matches, %d non matches)\n", count.get(), matched.get(), not_matched.get());
    }


    protected void fetchAddressesAndInvoke(SocketAddress dest, InetAddress bind_addr, String request, String passcode,
                                           long timeout, int ttl, boolean udp, boolean tcp) throws IOException {
        Consumer<ByteArray> on_rsp_handler=buf -> {
            String response=new String(buf.getArray(), 0, buf.getLength());
            try {
                Collection<SocketAddress> targets=parseAddresses(response, ((InetSocketAddress)dest).getPort());
                if(targets == null || targets.isEmpty())
                    return;
                for(SocketAddress target: targets) {
                    Requester req;
                    if(udp) {
                        if(verbose)
                            System.out.printf("%s sending UDP request to %s\n", PREFIX, target);
                        req=new UdpRequester(target, request, passcode, null).start(bind_addr, timeout, ttl);
                        req.run();
                    }
                    if(tcp) {
                        if(verbose)
                            System.out.printf("%s sending TCP request to %s\n", PREFIX, target);
                        req=new TcpRequester(target, request, passcode, null).start(bind_addr, timeout, ttl);
                        req.run();
                    }
                }
            }
            catch(Throwable t) {
            }
        };

        if(udp) {
            Requester r=new UdpRequester(dest, MEMBER_ADDRS, passcode, on_rsp_handler)
              .start(bind_addr, timeout, ttl);
            requesters.add(r);
            thread_pool.execute(r);
        }
        if(tcp) {
            Requester r=new TcpRequester(dest, MEMBER_ADDRS, passcode, on_rsp_handler)
              .start(bind_addr, timeout, ttl);
            requesters.add(r);
            thread_pool.execute(r);
        }
    }


    /**
     * Returns a list of addr:port responses. Counting the occurrences of a given address, e.g. 3 assumes we have 3
     * processes on the same host, yielding requests to host:port, host:port+1 and host_port+3. This is a quick-n-dirty
     * scheme and it will fail when processes don't occupy adjacent ports.
     */
    protected static Collection<SocketAddress> parseAddresses(String input, int port) throws Exception {
        final String ADDRS=MEMBER_ADDRS + "=";
        Map<InetAddress,Integer> map=new ConcurrentHashMap<>();
        Collection<SocketAddress> retval=new ArrayList<>();
        int start_index=-1, end_index=-1;
        if(input != null && (start_index=input.indexOf(ADDRS)) >= 0) {
            input=input.substring(start_index + ADDRS.length()).trim();
            end_index=input.indexOf('\n');
            if(end_index > 0)
                input=input.substring(0, end_index);
            List<String> rsps=Util.parseStringList(input,",");
            for(String tmp: rsps) {
                int index2=tmp.lastIndexOf(':');
                if(index2 != -1)
                    tmp=tmp.substring(0, index2);
                InetAddress key=InetAddress.getByName(tmp);
                Integer val=map.putIfAbsent(key, 1);
                if(val != null)
                    map.put(key, val+1);
            }
        }
        for(Map.Entry<InetAddress,Integer> entry: map.entrySet()) {
            InetAddress key=entry.getKey();
            int val=entry.getValue();
            for(int j=0; j < val; j++)
                retval.add(new InetSocketAddress(key, port+j));
        }
        return retval;
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


    public static void main(String[] args) throws Exception {
        InetAddress       bind_addr=null;
        StackType         ip_version=Util.getIpStackType();
        List<InetAddress> addrs=new ArrayList<>();
        int               port=0;
        int               ttl=32;
        long              timeout=500;
        StringBuilder     request=new StringBuilder();
        String            match=null;
        boolean           weed_out_duplicates=false, udp=true, tcp=false, verbose=false;
        String            passcode=null;


        for(int i=0; i < args.length; i++) {
            if("-addr".equals(args[i])) {
                InetAddress addr=Util.getByName(args[++i], ip_version);
                if(addr instanceof Inet6Address)
                    ip_version=StackType.IPv6;
                addrs.add(addr);
                args[i]=args[i-1]=null;
                continue;
            }
            if("-bind_addr".equals(args[i])) {
                bind_addr=Util.getByName(args[++i], ip_version);
                if(bind_addr instanceof Inet6Address)
                    ip_version=StackType.IPv6;
                args[i]=args[i-1]=null;
                continue;
            }
            if("-4".equals(args[i])) {
                ip_version=StackType.IPv4;
                args[i]=null;
                continue;
            }
            if("-6".equals(args[i])) {
                ip_version=StackType.IPv6;
                args[i]=null;
                continue;
            }
            if("-v".equals(args[i])) {
                verbose=true;
                args[i]=null;
            }
        }
        try {
            for(int i=0; i < args.length; i++) {
                if(args[i] == null)
                    continue;

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
                if("-cluster".equals(args[i])) {
                    String cluster=args[++i];
                    request.append("cluster=" + cluster + " ");
                    continue;
                }
                if("-udp".equals(args[i])) {
                    udp=Boolean.parseBoolean(args[++i]);
                    continue;
                }
                if("-tcp".equals(args[i])) {
                    tcp=Boolean.parseBoolean(args[++i]);
                    continue;
                }
                if("-help".equals(args[i]) || "-h".equals(args[i]) || "--help".equals(args[i])) {
                    help();
                    return;
                }
                request.append(args[i] + " ");
            }

            if(!udp && !tcp)
                throw new IllegalArgumentException("either UDP or TCP mode has to be enabled");
            if(tcp) udp=false;
            if(ip_version == StackType.IPv6 && bind_addr == null)
                bind_addr=Util.getLoopback(ip_version);

            Probe p=new Probe().verbose(verbose);
            if(addrs.isEmpty()) {
                if(udp) {
                    InetAddress mcast_addr=InetAddress.getByName(ip_version == StackType.IPv6?
                                                                   Global.DEFAULT_DIAG_ADDR_IPv6 : Global.DEFAULT_DIAG_ADDR);
                    if(!addrs.contains(mcast_addr))
                        addrs.add(mcast_addr);
                }
                if(tcp) {
                    InetAddress local=Util.getNonLoopbackAddress();
                    if(local != null && !addrs.contains(local))
                        addrs.add(local);
                }
            }
            if(port == 0)
                port=DEFAULT_DIAG_PORT;
            p.start(addrs, bind_addr, port, ttl, timeout, request.toString(), match, weed_out_duplicates, passcode, udp, tcp);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

    protected static void help() {
        System.out.println("Probe [-help] [-addr <addr>] [-4] [-6] [-bind_addr <addr>] " +
                             "[-port <port>] [-ttl <ttl>] [-timeout <timeout>] [-passcode <code>] [-weed_out_duplicates] " +
                             "[-cluster regexp-pattern] [-match pattern] [-udp true|false] [-tcp true|false] " +
                             "[-v] [key[=value]]*\n\n" +
                             "Examples:\n" +
                             "probe.sh keys // dumps all valid commands\n" +
                             "probe.sh jmx=NAKACK // dumps JMX info about all NAKACK protocols\n" +
                             "probe.sh op=STABLE.runMessageGarbageCollection // invokes the method in all STABLE protocols\n" +
                             "probe.sh jmx=UDP.oob,thread_pool // dumps all attrs of UDP starting with oob* or thread_pool*\n" +
                             "probe.sh jmx=FLUSH.bypass=true\n");
    }


    protected abstract class Requester implements Runnable {
        protected final SocketAddress dest;
        protected final String        request, passcode;
        protected Consumer<ByteArray> on_rsp;


        protected final Consumer<ByteArray> ON_RSP=buf -> {
            if(buf == null) {
                System.out.println("\n");
                return;
            }
            String response=new String(buf.getArray(), 0, buf.getLength());
            if(weed_out_duplicates && checkDuplicateResponse(response))
                return;
            count.incrementAndGet();
            if(matches(response, Probe.this.match)) {
                matched.incrementAndGet();
                System.out.printf("#%d (%d bytes):\n%s\n", count.get(), buf.getLength(), response);
            }
            else
                not_matched.incrementAndGet();
        };


        protected Requester(SocketAddress dest, String request, String passcode, Consumer<ByteArray> on_rsp) {
            this.dest=dest;
            this.request=request;
            this.passcode=passcode;
            this.on_rsp=on_rsp != null? on_rsp : ON_RSP;
        }

        protected abstract <T extends Requester> T start(InetAddress bind_addr, long timeout, int ttl) throws IOException;
        protected abstract <T extends Requester> T stop();
        protected abstract boolean                 isRunning();
        protected abstract <T extends Requester> T sendRequest(byte[] request) throws IOException;
        protected abstract ByteArray               fetchResponse();
        protected <T extends Requester> T          setResponseHandler(Consumer<ByteArray> rh) {this.on_rsp=rh; return (T)this;}


        public void run() {
            try {
                byte[] req=createRequest();
                sendRequest(req);
                // System.out.println("\n");
                while(isRunning()) {
                    ByteArray data=fetchResponse();
                    if(data == null)
                        break;
                    if(on_rsp != null)
                        on_rsp.accept(data);
                }
            }
            catch(Throwable t) {
                System.err.printf("failed sending request to %s: %s\n", dest, t);
            }
        }


        protected byte[] createRequest() throws IOException, NoSuchAlgorithmException {
            byte[] authenticationDigest=null;
            if(passcode != null) {
                long t1=(new Date()).getTime();
                double q1=Math.random();
                authenticationDigest=Util.createAuthenticationDigest(passcode, t1, q1);
            }
            byte[] queryPayload=request.getBytes();
            byte[] payload=queryPayload;
            if(authenticationDigest != null) {
                payload=new byte[authenticationDigest.length + queryPayload.length];
                System.arraycopy(authenticationDigest, 0, payload, 0, authenticationDigest.length);
                System.arraycopy(queryPayload, 0, payload, authenticationDigest.length, queryPayload.length);
            }
            return payload;
        }

    }

    protected class UdpRequester extends Requester {
        protected MulticastSocket sock;
        protected final byte[]    buf=new byte[70000];

        protected UdpRequester(SocketAddress dest, String request, String passcode, Consumer<ByteArray> on_rsp) {
            super(dest, request, passcode, on_rsp);
        }

        protected <T extends Requester> T start(InetAddress bind_addr, long timeout, int ttl) throws IOException {
            sock=new MulticastSocket();
            sock.setSoTimeout((int)timeout);
            sock.setTimeToLive(ttl);
            if(bind_addr != null)
                sock.setNetworkInterface(NetworkInterface.getByInetAddress(bind_addr));
            return (T)this;
        }

        protected <T extends Requester> T stop() {
            Util.close(sock);
            return (T)this;
        }

        protected boolean isRunning() {
            return sock != null && !sock.isClosed();
        }

        protected <T extends Requester> T sendRequest(byte[] request) throws IOException {
            DatagramPacket probe=new DatagramPacket(request, 0, request.length, dest);
            sock.send(probe);
            return (T)this;
        }

        protected ByteArray fetchResponse() {
            DatagramPacket rsp=new DatagramPacket(buf, 0, buf.length);
            try {
                sock.receive(rsp);
                return new ByteArray(rsp.getData(), 0, rsp.getLength());
            }
            catch(Throwable t) {
                return null;
            }
        }
    }

    protected class TcpRequester extends Requester {
        protected Socket       sock;
        protected InputStream  in;
        protected OutputStream out;


        protected TcpRequester(SocketAddress dest, String request, String passcode,
                               Consumer<ByteArray> on_rsp) {
            super(dest, request, passcode, on_rsp);
        }

        protected <T extends Requester> T start(InetAddress bind_addr, long timeout, int ttl) throws IOException {
            sock=new Socket();
            sock.setSoTimeout((int)timeout);
            sock.bind(new InetSocketAddress(bind_addr, 0));
            sock.connect(dest);
            in=sock.getInputStream();
            out=sock.getOutputStream();
            return (T)this;
        }

        protected <T extends Requester> T stop() {
            Util.close(sock,in,out);
            return (T)this;
        }

        protected boolean isRunning() {
            return sock != null && !sock.isClosed();
        }

        protected <T extends Requester> T sendRequest(byte[] request) throws IOException {
            out.write(request);
            out.write('\n');
            // out.flush();
            return (T)this;
        }

        protected ByteArray fetchResponse() {
            byte[] buf=new byte[1024];
            int    index=0;

            for(;;) {
                try {
                    int bytes_read=in.read(buf, index, buf.length - index);
                    if(bytes_read == -1) {
                        if(index > 0)
                            break;
                        return null;
                    }
                    index+=bytes_read;
                    if(index >= buf.length) {
                        byte[] tmp=new byte[buf.length + 1024];
                        System.arraycopy(buf, 0, tmp, 0, index);
                        buf=tmp;
                    }
                }
                catch(IOException e) {
                    break;
                }
            }
            return new ByteArray(buf, 0, index);
        }
    }
}
