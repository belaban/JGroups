
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests performance of unicast RPCs between a caller and a recipient
 *
 * @author Bela Ban
 */
public class UnicastTestRpc extends ReceiverAdapter {
    private JChannel                  channel;
    private Address                   local_addr;
    private RpcDispatcher             disp;
    private String                    groupname="UTestRpc";
    private boolean                   sync=false, oob=false, anycasting=false;
    private int                       num_threads=1;
    private int                       num_msgs=50000, msg_size=1000, print=num_msgs / 10;
    private int                       anycast_count=1;
    private final Collection<Address> anycast_mbrs=new ArrayList<Address>();
    private Address                   destination=null;

    private static final Method       RECEIVE;
    private static final Method[]     METHODS=new Method[1];

    protected final AtomicInteger     num_requests=new AtomicInteger(0);



    static {
        try {
            RECEIVE=UnicastTestRpc.class.getMethod("receiveData", byte[].class);
            METHODS[0]=RECEIVE;
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, final String name, String cluster_name) throws Exception {
        if(cluster_name != null)
            groupname=cluster_name;
        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        disp=new RpcDispatcher(channel, null, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return METHODS[id];
            }
        });
        disp.setRequestMarshaller(new CustomMarshaller());
        channel.connect(groupname);
        local_addr=channel.getAddress();

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }
    }

    void stop() {
        if(disp != null)
            disp.stop();
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }


    public void receiveData(byte[] buffer) {
    }


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Set sender threads (" + num_threads +
                               ") [4] Set num msgs (" + num_msgs + ") " +
                               "\n[5] Set msg size (" + Util.printBytes(msg_size) + ")" +
                               " [6] Set anycast count (" + anycast_count + ")" +
                               "\n[o] Toggle OOB (" + oob + ") [s] Toggle sync (" + sync + ") [a] Toggle anycasting (" + anycasting + ")" +
                               "\n[q] Quit\n");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                try {
                    invokeRpcs();
                }
                catch(Throwable t) {
                    System.err.println(t);
                }
                break;
            case '2':
                printView();
                break;
            case '3':
                setSenderThreads();
                break;
            case '4':
                setNumMessages();
                break;
            case '5':
                setMessageSize();
                break;
            case '6':
                setAnycastCount();
                break;
            case 'o':
                oob=!oob;
                System.out.println("oob=" + oob);
                break;
            case 's':
                sync=!sync;
                System.out.println("sync=" + sync);
                break;
            case 'a':
                anycasting=!anycasting;
                System.out.println("anycasting=" + anycasting);
                break;
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }




    void invokeRpcs() throws Throwable {
        if(anycasting) {
            populateAnycastList(channel.getView());
        }
        else {
            if((destination=getReceiver()) == null) {
                System.err.println("UnicastTest.invokeRpcs(): receiver is null, cannot send messages");
                return;
            }
        }

        num_requests.set(0);

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) + " on " +
                             (anycasting? anycast_mbrs : destination) + ", sync=" + sync + ", oob=" + oob + ", anycasting=" + anycasting);
        
        // The first call needs to be synchronous with OOB !
        RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 15000, anycasting, null);
        if(sync) options.setFlags(Message.Flag.DONT_BUNDLE);
        if(oob) options.setFlags(Message.Flag.OOB);

        options.setMode(sync? ResponseMode.GET_ALL : ResponseMode.GET_NONE);

        final CountDownLatch latch=new CountDownLatch(1);
        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++) {
            if(anycasting)
                invokers[i]=new Invoker(anycast_mbrs, options, latch);
            else
                invokers[i]=new Invoker(destination, options, latch);
            invokers[i].setName("invoker-" + i);
            invokers[i].start();
        }

        long start=System.currentTimeMillis();
        latch.countDown();

        for(Invoker invoker: invokers)
            invoker.join();
        long time=System.currentTimeMillis() - start;

        System.out.println("done invoking " + num_msgs + " in " + destination);

        double time_per_req=time / (double)num_msgs;
        double reqs_sec=num_msgs / (time / 1000.0);
        double throughput=num_msgs * msg_size / (time / 1000.0);
        System.out.println(Util.bold("\ninvoked " + num_msgs + " requests in " + time + " ms: " + time_per_req + " ms/req, " +
                                       String.format("%.2f", reqs_sec) + " reqs/sec, " + Util.printBytes(throughput) + "/sec\n"));
    }

    void setSenderThreads() throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        int old=this.num_threads;
        this.num_threads=threads;
        System.out.println("sender threads set to " + num_threads + " (from " + old + ")");
    }

    void setNumMessages() throws Exception {
        num_msgs=Util.readIntFromStdin("Number of RPCs: ");
        System.out.println("Set num_msgs=" + num_msgs);
        print=num_msgs / 10;
    }

    void setMessageSize() throws Exception {
        msg_size=Util.readIntFromStdin("Message size: ");
        System.out.println("set msg_size=" + msg_size);
    }

    void setAnycastCount() throws Exception {
        int tmp=Util.readIntFromStdin("Anycast count: ");
        View view=channel.getView();
        if(tmp > view.size()) {
            System.err.println("anycast count must be smaller or equal to the view size (" + view + ")\n");
            return;
        }

        anycast_count=tmp;
        System.out.println("set anycast_count=" + anycast_count);
    }

    void populateAnycastList(View view) {
        if(!anycasting) return;
        anycast_mbrs.clear();
        List<Address> mbrs=view.getMembers();
        int index=mbrs.indexOf(local_addr);
        for(int i=index + 1; i < index + 1 + anycast_count; i++) {
            int new_index=i % mbrs.size();
            anycast_mbrs.add(mbrs.get(new_index));
        }
        System.out.println("local_addr=" + local_addr + ", anycast_mbrs = " + anycast_mbrs);
    }

    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }



    private Address getReceiver() {
        try {
            List<Address> mbrs=new ArrayList<Address>(channel.getView().getMembers());
            List<String> site_names=getSites(channel);
            for(String site_name: site_names) {
                try {
                    SiteMaster sm=new SiteMaster(site_name);
                    mbrs.add(sm);
                }
                catch(Throwable t) {
                    System.err.println("failed creating site master: " + t);
                }
            }

            System.out.println("pick receiver from the following members:");
            int i=0;
            for(Address mbr: mbrs) {
                if(mbr.equals(channel.getAddress()))
                    System.out.println("[" + i + "]: " + mbr + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbr);
                i++;
            }
            System.out.flush();
            System.in.skip(System.in.available());
            BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
            String str=reader.readLine().trim();
            int index=Integer.parseInt(str);
            return mbrs.get(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }


    protected static List<String> getSites(JChannel channel) {
        RELAY2 relay=(RELAY2)channel.getProtocolStack().findProtocol(RELAY2.class);
        return relay != null? relay.siteNames() : Collections.<String>emptyList();
    }


    private class Invoker extends Thread {
        private final Address             dest;
        private final Collection<Address> dests;
        private final RequestOptions      options;
        private final CountDownLatch      latch;


        public Invoker(Address dest, RequestOptions options, CountDownLatch latch) {
            this.dest=dest;
            this.latch=latch;
            this.dests=null;
            this.options=options;
        }

        public Invoker(Collection<Address> dests, RequestOptions options, CountDownLatch latch) {
            this.latch=latch;
            this.dest=null;
            this.dests=dests;
            this.options=options;
        }

        public void run() {
            byte[] buf=new byte[msg_size];
            Object[] args={buf};
            MethodCall call=new MethodCall((short)0, args);

            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            for(;;) {
                int i=num_requests.incrementAndGet();
                if(i > num_msgs)
                    break;
                try {
                    if(dests != null)
                        disp.callRemoteMethods(dests, call, options);
                    else
                        disp.callRemoteMethod(dest, call, options);
                    if(print > 0 && i % print == 0)
                        System.out.println("-- invoked " + i);
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }
    }


    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public Buffer objectToBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            if(call.getId() == 0) {
                byte[] arg=(byte[])call.getArgs()[0];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + arg.length);
                buf.put((byte)0).putInt(arg.length).put(arg, 0, arg.length);
                return new Buffer(buf.array());
            }
            else
                throw new IllegalStateException("method " + call.getMethod() + " not known");
        }

        public Object objectFromBuffer(byte[] buffer, int offset, int length) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer, offset, length);

            byte type=buf.get();
            switch(type) {
                case 0:
                    int len=buf.getInt();
                    byte[] arg=new byte[len];
                    buf.get(arg, 0, arg.length);
                    return new MethodCall((short)0, arg);
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
        }
    }


    public static void main(String[] args) {
        String props=null;
        String name=null;
        String cluster_name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-cluster".endsWith(args[i])) {
                cluster_name=args[++i];
                continue;
            }
            help();
            return;
        }

        UnicastTestRpc  test=null;
        try {
            test=new UnicastTestRpc();
            test.init(props, name, cluster_name);
            test.eventLoop();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UnicastTestRpc [-help] [-props <props>] [-name name] [-cluster name]");
    }


}