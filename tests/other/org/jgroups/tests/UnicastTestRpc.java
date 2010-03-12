
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.blocks.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTestRpc extends ReceiverAdapter {
    private JChannel channel;
    private Address local_addr;
    private RpcDispatcher disp;
    static final String groupname="UnicastTest-Group";
    private long sleep_time=0;
    private boolean sync=false, oob=false, anycasting=false;
    private int num_threads=1;
    private int num_msgs=50000, msg_size=1000;
    private int anycast_count=1;
    private final Collection<Address> anycast_mbrs=new ArrayList<Address>();
    private Address destination=null;

    private boolean started=false;
    private long start=0, stop=0;
    private AtomicInteger current_value=new AtomicInteger(0);
    private int num_values=0, print;
    private AtomicLong total_bytes=new AtomicLong(0);

    private static final Method START;
    private static final Method RECEIVE;
    private static final Method[] METHODS=new Method[2];

    private static final Class<?>[] unicast_protocols=new Class<?>[]{UNICAST.class, UNICAST2.class};

    private final AtomicInteger COUNTER=new AtomicInteger(1);


    long tot=0;
    int num_reqs=0;

    static {
        try {
            START=UnicastTestRpc.class.getMethod("startTest", int.class);
            RECEIVE=UnicastTestRpc.class.getMethod("receiveData", long.class, byte[].class);
            METHODS[0]=START;
            METHODS[1]=RECEIVE;
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, long sleep_time, boolean sync, boolean oob, String name) throws Exception {
        this.sleep_time=sleep_time;
        this.sync=sync;
        this.oob=oob;
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

    public void startTest(int num_values) {
        if(started) {
            System.err.println("UnicastTest.run(): received START data, but am already processing data");
        }
        else {
            started=true;
            current_value.set(0); // first value to be received
            total_bytes.set(0);
            this.num_values=num_values;
            print=num_values / 10;

            tot=0; num_reqs=0;

            start=System.currentTimeMillis();
        }
    }

    public long receiveData(long value, byte[] buffer) {
        long diff=System.currentTimeMillis() - value;
        tot+=diff;
        num_reqs++;

        long new_val=current_value.incrementAndGet();
        total_bytes.addAndGet(buffer.length);
        if(print > 0 && new_val % print == 0)
            System.out.println("received " + current_value);
        if(new_val >= num_values) {
            stop=System.currentTimeMillis();
            long total_time=stop - start;
            long msgs_per_sec=(long)(num_values / (total_time / 1000.0));
            double throughput=total_bytes.get() / (total_time / 1000.0);
            System.out.println("\n-- received " + num_values + " messages in " + total_time +
                    " ms (" + msgs_per_sec + " messages/sec, " + Util.printBytes(throughput) + " / sec)");



            double time_per_req=(double)tot / num_reqs;
            System.out.println("received " + num_reqs + " requests in " + tot + " ms, " + time_per_req +
                    " ms / req (only request)\n");

            started=false;
        }
        return System.currentTimeMillis();
    }


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print conns " +
                    "[4] Trash conn [5] Trash all conns" +
                    "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    " [9] Set anycast count (" + anycast_count + ")" +
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
                printConnections();
                break;
            case '4':
                removeConnection();
                break;
            case '5':
                removeAllConnections();
                break;
            case '6':
                setSenderThreads();
                break;
            case '7':
                setNumMessages();
                break;
            case '8':
                setMessageSize();
                break;
            case '9':
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

    private void printConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(unicast_protocols);
        if(prot instanceof UNICAST)
            System.out.println("connections:\n" + ((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println("connections:\n" + ((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            Protocol prot=channel.getProtocolStack().findProtocol(unicast_protocols);
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(unicast_protocols);
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
    }


    void invokeRpcs() throws Throwable {
        if(num_threads > 1 && num_msgs % num_threads != 0) {
            System.err.println("num_msgs (" + num_msgs + " ) has to be divisible by num_threads (" + num_threads + ")");
            return;
        }

        if(anycasting) {
            populateAnycastList(channel.getView());
        }
        else {
            if((destination=getReceiver()) == null) {
                System.err.println("UnicastTest.invokeRpcs(): receiver is null, cannot send messages");
                return;
            }
        }

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) + " on " +
                (anycasting? anycast_mbrs : destination) + ", sync=" + sync + ", oob=" + oob + ", anycasting=" + anycasting);
        
        // The first call needs to be synchronous with OOB !
        RequestOptions options=new RequestOptions(Request.GET_ALL, 0, anycasting, null);
        if(sync) options.setFlags(Message.DONT_BUNDLE);
        if(oob) options.setFlags(Message.OOB);

        if(anycasting)
            disp.callRemoteMethods(anycast_mbrs, new MethodCall((short)0, num_msgs), options);
        else
            disp.callRemoteMethod(destination, new MethodCall((short)0, num_msgs), options);
        options.setMode(sync? Request.GET_ALL : Request.GET_NONE);

        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++) {
            if(anycasting)
                invokers[i]=new Invoker(anycast_mbrs, options, num_msgs / num_threads);
            else
                invokers[i]=new Invoker(destination, options, num_msgs / num_threads);
        }
        for(Invoker invoker: invokers)
            invoker.start();
        for(Invoker invoker: invokers)
            invoker.join();

        System.out.println("done invoking " + num_msgs + " in " + destination);
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
        Vector<Address> mbrs=view.getMembers();
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
            Vector<Address> mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(channel.getAddress()))
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i) + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i));
            }
            System.out.flush();
            System.in.skip(System.in.available());
            BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
            String str=reader.readLine().trim();
            int index=Integer.parseInt(str);
            return mbrs.elementAt(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }

    private class Invoker extends Thread {
        private final Address             dest;
        private final Collection<Address> dests;
        private final RequestOptions      options;
        private final int                 number_of_msgs;


        long total_req=0, total_rsp=0;

        public Invoker(Address dest, RequestOptions options, int number_of_msgs) {
            this.dest=dest;
            this.dests=null;
            this.options=options;
            this.number_of_msgs=number_of_msgs;
            setName("Invoker-" + COUNTER.getAndIncrement());
        }

        public Invoker(Collection<Address> dests, RequestOptions options, int number_of_msgs) {
            this.dest=null;
            this.dests=dests;
            this.options=options;
            this.number_of_msgs=number_of_msgs;
            setName("Invoker-" + COUNTER.getAndIncrement());
        }

        public void run() {
            byte[] buf=new byte[msg_size];
            Object[] args=new Object[]{0, buf};
            MethodCall call=new MethodCall((short)1, args);

            //if(anycasting && sync)
               //  options.setMode(Request.GET_FIRST);

            for(int i=1; i <= number_of_msgs; i++) {
                Object retval=null;
                try {
                    long start=System.currentTimeMillis();
                    args[0]=start;
                    if(dests != null)
                        disp.callRemoteMethods(dests, call, options);
                    else
                        retval=disp.callRemoteMethod(dest, call, options);
                    long current_time=System.currentTimeMillis();
                    long diff=current_time - start;
                    total_req+=diff;

                    if(sync) {
                        if(retval instanceof Long) {
                            diff=System.currentTimeMillis() - (Long)retval;
                            total_rsp+=diff;
                        }
                    }

                    if(print > 0 && i % print == 0)
                        System.out.println("-- invoked " + i);
                    if(sleep_time > 0)
                        Util.sleep(sleep_time);
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }

            double time_per_req=total_req / (double)number_of_msgs;
            System.out.println("\ninvoked " + number_of_msgs + " requests in " + total_req + " ms: " + time_per_req +
                    " ms / req (entire request)");

            if(sync) {
                double time_per_rsp=total_rsp / (double)number_of_msgs;
                System.out.println("received " + number_of_msgs + " responses in " + total_rsp + " ms: " + time_per_rsp +
                        " ms / rsp (only response)\n");
            }


        }
    }


    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public byte[] objectToByteBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            if(call.getId() == 0) {
                Integer arg=(Integer)call.getArgs()[0];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE);
                buf.put((byte)0).putInt(arg);
                return buf.array();
            }
            else if(call.getId() == 1) {
                Long arg=(Long)call.getArgs()[0];
                byte[] arg2=(byte[])call.getArgs()[1];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + Global.LONG_SIZE + arg2.length);
                buf.put((byte)1).putLong(arg).putInt(arg2.length).put(arg2, 0, arg2.length);
                return buf.array();
            }
            else
                throw new IllegalStateException("method " + call.getMethod() + " not known");
        }

        public Object objectFromByteBuffer(byte[] buffer) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer);

            byte type=buf.get();
            switch(type) {
                case 0:
                    int arg=buf.getInt();
                    return new MethodCall((short)0, arg);
                case 1:
                    Long longarg=buf.getLong();
                    int len=buf.getInt();
                    byte[] arg2=new byte[len];
                    buf.get(arg2, 0, arg2.length);
                    return new MethodCall((short)1, longarg, arg2);
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
        }
    }


    public static void main(String[] args) {
        long sleep_time=0;
        String props=null;
        boolean sync=false;
        boolean oob=false;
        String name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-sync".equals(args[i])) {
                sync=true;
                continue;
            }
            if("-oob".equals(args[i])) {
                oob=true;
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        UnicastTestRpc  test=null;
        try {
            test=new UnicastTestRpc();
            test.init(props, sleep_time, sync, oob, name);
            test.eventLoop();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UnicastTestRpc [-help] [-props <props>] [-name name] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep]");
    }


}