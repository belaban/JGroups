

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Tests transfer of large states. Start first instance with -provider flag and -size flag (default = 1MB).
 * The start second instance without these flags: it should acquire the state from the first instance. Possibly
 * tracing should be turned on for FRAG to see the fragmentation taking place, e.g.:
 * <pre>
 * trace1=FRAG DEBUG STDOUT
 * </pre><br>
 * Note that because fragmentation might generate a lot of small fragments at basically the same time (e.g. size1MB,
 * FRAG.frag-size=4096 generates a lot of fragments), the send buffer of the unicast socket in UDP might be overloaded,
 * causing it to drop some packets (default size is 8096 bytes). Therefore the send (and receive) buffers for the unicast
 * socket have been increased (see ucast_send_buf_size and ucast_recv_buf_size below).<p>
 * If we didn't do this, we would have some retransmission, slowing the state transfer down.
 *
 * @author Bela Ban Dec 13 2001
 */
public class LargeState implements Receiver {
    JChannel channel;
    byte[]   state=null;
    boolean  rc=false;
    String   props;
    long     start, stop;
    boolean  provider=true, provider_fails=false, requester_fails=false;
    int      size=100000;
    int      total_received=0;
    long     delay=0;


    public void start(boolean provider, int size, String props, boolean provider_fails,
                      boolean requester_fails, long delay, String name) throws Exception {
        this.provider=provider;
        this.provider_fails=provider_fails;
        this.requester_fails=requester_fails;
        this.delay=delay;
        channel=new JChannel(props);
        channel.setReceiver(this);
        if(name != null)
            channel.setName(name);
        channel.connect("TestChannel");
        MBeanServer server=Util.getMBeanServer();
        if(server == null)
            throw new Exception("No MBeanServers found;" +
                                  "\nLargeState needs to be run with an MBeanServer present, or inside JDK 5");
        JmxConfigurator.registerChannel((JChannel)channel, server, "jgroups", channel.getClusterName(), true);
        System.out.println("-- connected to channel");

        if(provider) {
            this.size=size;
            System.out.println("Waiting for other members to join and fetch large state");
            for(;;) {
                Util.sleep(10000);
            }
        }
        start=System.currentTimeMillis();
        try {
            channel.getState(null, 0);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
            Util.close(channel);
        }
    }


    static byte[] createLargeState(int size) {
        return new byte[size];
    }

    public void receive(Message msg) {
        System.out.println("-- received msg " + msg.getObject() + " from " + msg.getSrc());
    }

    public void viewAccepted(View new_view) {
        if(provider)
            System.out.println("-- view: " + new_view);
    }


    public void setState(InputStream istream) throws Exception {
        total_received=0;
        int received=0;
        while(true) {
            byte[] buf=new byte[10000];
            received=istream.read(buf);
            if(received < 0)
                break;
            if(delay > 0)
                Util.sleep(delay);
            total_received+=received;
            if(requester_fails)
                throw new Exception("booom - requester failed");
        }

        stop=System.currentTimeMillis();
        System.out.println("<-- received " + Util.printBytes(total_received) + " in " + (stop-start) + "ms");
    }

    public void getState(OutputStream ostream) throws Exception {
        int frag_size=size / 10;
        long bytes=0;
        for(int i=0; i < 10; i++) {
            byte[] buf=new byte[frag_size];
            ostream.write(buf);
            bytes+=buf.length;
            if(provider_fails)
                throw new Exception("booom - provider failed");
            if(delay > 0)
                Util.sleep(delay);
        }
        int remaining=size - (10 * frag_size);
        if(remaining > 0) {
            byte[] buf=new byte[remaining];
            ostream.write(buf);
            bytes+=buf.length;
        }
        System.out.println("--> wrote " + Util.printBytes(bytes));
    }


    public static void main(String[] args) {
        boolean provider=false, provider_fails=false, requester_fails=false;
        int     size=1024 * 1024;
        String  props=null;
        long    delay=0;
        String  name=null;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-provider".equals(args[i])) {
            	provider=true;
                continue;
            }
            if("-provider_fails".equals(args[i])) {
                provider_fails=true;
                continue;
            }
            if("-requester_fails".equals(args[i])) {
                requester_fails=true;
                continue;
            }
            if("-size".equals(args[i])) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-delay".equals(args[i])) {
                delay=Long.parseLong(args[++i]);
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }


        try {
            new LargeState().start(provider, size, props, provider_fails, requester_fails, delay, name);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    static void help() {
        System.out.println("LargeState [-help] [-size <size of state in bytes] [-provider] [-name name] " +
                             "[-props <properties>] [-provider_fails] [-requester_fails] [-delay <ms>]");
    }

}
