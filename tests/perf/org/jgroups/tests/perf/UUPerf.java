package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.util.*;

import javax.management.MBeanServer;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests the UNICAST by invoking unicast RPCs from all the nodes to a single node.
 * Mimics state transfer in Infinispan.
 * @author Dan Berindei
 */
public class UUPerf extends ReceiverAdapter {
    private JChannel channel;
    private Address local_addr;
    private RpcDispatcher disp;
    static final String groupname="uuperf";
    private final List<Address> members=new ArrayList<>();


    // ============ configurable properties ==================
    private boolean sync=true, oob=true;
    private int num_threads=2;
    private int num_msgs=1, msg_size=(int)(4.5 * 1000 * 1000);
    // =======================================================

    private static final Method[] METHODS=new Method[15];

    private static final short START=0;
    private static final short SET_OOB=1;
    private static final short SET_SYNC=2;
    private static final short SET_NUM_MSGS=3;
    private static final short SET_NUM_THREADS=4;
    private static final short SET_MSG_SIZE=5;
    private static final short APPLY_STATE=6;
    private static final short GET_CONFIG=10;

    private final AtomicInteger COUNTER=new AtomicInteger(1);
    private byte[] GET_RSP=new byte[msg_size];


    static NumberFormat f;


    static {
        try {
            METHODS[START]=UUPerf.class.getMethod("startTest");
            METHODS[SET_OOB]=UUPerf.class.getMethod("setOOB",boolean.class);
            METHODS[SET_SYNC]=UUPerf.class.getMethod("setSync",boolean.class);
            METHODS[SET_NUM_MSGS]=UUPerf.class.getMethod("setNumMessages",int.class);
            METHODS[SET_NUM_THREADS]=UUPerf.class.getMethod("setNumThreads",int.class);
            METHODS[SET_MSG_SIZE]=UUPerf.class.getMethod("setMessageSize",int.class);
            METHODS[APPLY_STATE]=UUPerf.class.getMethod("applyState",byte[].class);
            METHODS[GET_CONFIG]=UUPerf.class.getMethod("getConfig");

            ClassConfigurator.add((short)12000,Results.class);
            f=NumberFormat.getNumberInstance();
            f.setGroupingUsed(false);
            f.setMinimumFractionDigits(2);
            f.setMaximumFractionDigits(2);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, String name) throws Throwable {
        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        disp=new RpcDispatcher(channel,null,this,this);
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
            JmxConfigurator.registerChannel(channel,server,"jgroups",channel.getClusterName(),true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }

        if(members.size() < 2)
            return;
        Address coord=members.get(0);
        ConfigOptions config=(ConfigOptions)disp.callRemoteMethod(coord,new MethodCall(GET_CONFIG),new RequestOptions(ResponseMode.GET_ALL,5000));
        if(config != null) {
            this.oob=config.oob;
            this.sync=config.sync;
            this.num_threads=config.num_threads;
            this.num_msgs=config.num_msgs;
            this.msg_size=config.msg_size;
            System.out.println("Fetched config from " + coord + ": " + config);
        }
        else
            System.err.println("failed to fetch config from " + coord);
    }

    void stop() {
        if(disp != null)
            disp.stop();
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
    }

    // =================================== callbacks ======================================

    public Results startTest() throws Throwable {
        if(members.indexOf(local_addr) == members.size() - 1) {
            System.out.println("This is the joiner, not sending any state");
            return new Results(0,0);
        }

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) + ", sync=" + sync + ", oob=" + oob);
        final AtomicInteger num_msgs_sent=new AtomicInteger(0);

        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++)
            invokers[i]=new Invoker(members,num_msgs,num_msgs_sent);

        long start=System.currentTimeMillis();
        for(Invoker invoker : invokers)
            invoker.start();

        for(Invoker invoker : invokers) {
            invoker.join();
        }

        long total_time=System.currentTimeMillis() - start;
        System.out.println("done (in " + total_time + " ms)");
        return new Results(num_msgs_sent.get(),total_time);
    }


    public void setOOB(boolean oob) {
        this.oob=oob;
        System.out.println("oob=" + oob);
    }

    public void setSync(boolean val) {
        this.sync=val;
        System.out.println("sync=" + sync);
    }

    public void setNumMessages(int num) {
        num_msgs=num;
        System.out.println("num_msgs = " + num_msgs);
    }

    public void setNumThreads(int num) {
        num_threads=num;
        System.out.println("num_threads = " + num_threads);
    }

    public void setMessageSize(int num) {
        msg_size=num;
        System.out.println("msg_size = " + msg_size);
    }

    public static void applyState(byte[] val) {
        System.out.println("-- applyState(): " + Util.printBytes(val.length));
    }

    public ConfigOptions getConfig() {
        return new ConfigOptions(oob,sync,num_threads,num_msgs,msg_size);
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            c=Util.keyPress("[1] Send msgs [2] Print view [3] Print conns " +
                              "[4] Trash conn [5] Trash all conns" +
                              "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                              "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                              "\n[o] Toggle OOB (" + oob + ") [s] Toggle sync (" + sync + ")" +
                              "\n[q] Quit\n");
            switch(c) {
                case -1:
                    break;
                case '1':
                    try {
                        startBenchmark();
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
                case 'o':
                    boolean new_value=!oob;
                    disp.callRemoteMethods(null,new MethodCall(SET_OOB,new_value),RequestOptions.SYNC());
                    break;
                case 's':
                    boolean new_val=!sync;
                    disp.callRemoteMethods(null,new MethodCall(SET_SYNC,new_val),RequestOptions.SYNC());
                    break;
                case 'q':
                    channel.close();
                    return;
                case '\n':
                case '\r':
                    break;
                default:
                    break;
            }
        }
    }

    private void printConnections() {
        AbstractProtocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            System.out.println("connections:\n" + ((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println("connections:\n" + ((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            AbstractProtocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        AbstractProtocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
    }


    /**
     * Kicks off the benchmark on all cluster nodes
     */
    void startBenchmark() throws Throwable {
        RequestOptions options=new RequestOptions(ResponseMode.GET_ALL,0);
        options.setFlags(Message.Flag.OOB,Message.Flag.DONT_BUNDLE);
        RspList<Object> responses=disp.callRemoteMethods(null,new MethodCall(START),options);

        long total_reqs=0;
        long total_time=0;

        System.out.println("\n======================= Results: ===========================");
        for(Map.Entry<Address,Rsp<Object>> entry : responses.entrySet()) {
            Address mbr=entry.getKey();
            Rsp rsp=entry.getValue();
            Results result=(Results)rsp.getValue();
            total_reqs+=result.num_msgs;
            total_time+=result.time;
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec=total_reqs / (total_time / 1000.0);
        double throughput=total_reqs_sec * msg_size;
        double ms_per_req=total_time / (double)total_reqs;
        AbstractProtocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        System.out.println("\n");
        System.out.println(Util.bold("Average of " + f.format(total_reqs_sec) + " requests / sec (" +
                                       Util.printBytes(throughput) + " / sec), " +
                                       f.format(ms_per_req) + " ms /request (prot=" + prot.getName() + ")"));
        System.out.println("\n\n");
    }


    void setSenderThreads() throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        disp.callRemoteMethods(null,new MethodCall(SET_NUM_THREADS,threads),RequestOptions.SYNC());
    }

    void setNumMessages() throws Exception {
        int tmp=Util.readIntFromStdin("Number of RPCs: ");
        disp.callRemoteMethods(null,new MethodCall(SET_NUM_MSGS,tmp),RequestOptions.SYNC());
    }

    void setMessageSize() throws Exception {
        int tmp=Util.readIntFromStdin("Message size: ");
        disp.callRemoteMethods(null,new MethodCall(SET_MSG_SIZE,tmp),RequestOptions.SYNC());
    }


    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }


    /**
     * Picks the next member in the view
     */
    private Address getReceiver() {
        try {
            List<Address> mbrs=channel.getView().getMembers();
            int index=mbrs.indexOf(local_addr);
            int new_index=index + 1 % mbrs.size();
            return mbrs.get(new_index);
        }
        catch(Exception e) {
            System.err.println("UPerf.getReceiver(): " + e);
            return null;
        }
    }

    private class Invoker extends Thread {
        private final List<Address> dests=new ArrayList<>();
        private final int num_msgs_to_send;
        private final AtomicInteger num_msgs_sent;


        public Invoker(Collection<Address> dests, int num_msgs_to_send, AtomicInteger num_msgs_sent) {
            this.num_msgs_sent=num_msgs_sent;
            this.dests.addAll(dests);
            this.num_msgs_to_send=num_msgs_to_send;
            setName("Invoker-" + COUNTER.getAndIncrement());
        }


        public void run() {
            final byte[] buf=new byte[msg_size];
            Object[] apply_state_args={buf};
            MethodCall apply_state_call=new MethodCall(APPLY_STATE,apply_state_args);
            RequestOptions apply_state_options=new RequestOptions(sync? ResponseMode.GET_ALL : ResponseMode.GET_NONE,400000,true,null);

            if(oob) {
                apply_state_options.setFlags(Message.Flag.OOB);
            }
            if(sync) {
                // apply_state_options.setFlags(Message.Flag.DONT_BUNDLE,Message.NO_FC);
                apply_state_options.setFlags(Message.Flag.DONT_BUNDLE);
            }

            apply_state_options.setFlags(Message.Flag.RSVP);


            while(true) {
                long i=num_msgs_sent.getAndIncrement();
                if(i >= num_msgs_to_send)
                    break;

                try {
                    Address target=pickApplyStateTarget();
                    apply_state_args[0]=buf;
                    disp.callRemoteMethod(target,apply_state_call,apply_state_options);
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }

        private Address pickApplyStateTarget() {
            return dests.get(dests.size() - 1);
        }
    }


    public static class Results implements Streamable {
        long num_msgs=0;
        long time=0;

        public Results() {

        }

        public Results(int num_msgs, long time) {
            this.num_msgs=num_msgs;
            this.time=time;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(num_msgs);
            out.writeLong(time);
        }

        public void readFrom(DataInput in) throws Exception {
            num_msgs=in.readLong();
            time=in.readLong();
        }

        public String toString() {
            long total_reqs=num_msgs;
            double total_reqs_per_sec=total_reqs / (time / 1000.0);

            return f.format(total_reqs_per_sec) + " reqs/sec (" + num_msgs + " APPLY_STATEs total)";
        }
    }


    public static class ConfigOptions implements Streamable {
        private boolean sync, oob;
        private int num_threads;
        private int num_msgs, msg_size;

        public ConfigOptions() {
        }

        public ConfigOptions(boolean oob, boolean sync, int num_threads, int num_msgs, int msg_size) {
            this.oob=oob;
            this.sync=sync;
            this.num_threads=num_threads;
            this.num_msgs=num_msgs;
            this.msg_size=msg_size;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeBoolean(oob);
            out.writeBoolean(sync);
            out.writeInt(num_threads);
            out.writeInt(num_msgs);
            out.writeInt(msg_size);
        }

        public void readFrom(DataInput in) throws Exception {
            oob=in.readBoolean();
            sync=in.readBoolean();
            num_threads=in.readInt();
            num_msgs=in.readInt();
            msg_size=in.readInt();
        }

        public String toString() {
            return "oob=" + oob + ", sync=" + sync +
              ", num_threads=" + num_threads + ", num_msgs=" + num_msgs + ", msg_size=" + msg_size;
        }
    }


    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public Buffer objectToBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            ByteBuffer buf;
            switch(call.getId()) {
                case START:
                case GET_CONFIG:
                    buf=ByteBuffer.allocate(Global.BYTE_SIZE);
                    buf.put((byte)call.getId());
                    return new Buffer(buf.array());
                case SET_OOB:
                case SET_SYNC:
                    return new Buffer(booleanBuffer(call.getId(),(Boolean)call.getArgs()[0]));
                case SET_NUM_MSGS:
                case SET_NUM_THREADS:
                case SET_MSG_SIZE:
                    return new Buffer(intBuffer(call.getId(),(Integer)call.getArgs()[0]));
                case APPLY_STATE:
                    byte[] arg=(byte[])call.getArgs()[0];
                    buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + arg.length);
                    buf.put((byte)call.getId()).putInt(arg.length).put(arg,0,arg.length);
                    return new Buffer(buf.array());
                default:
                    throw new IllegalStateException("method " + call.getMethod() + " not known");
            }
        }


        public Object objectFromBuffer(byte[] buffer, int offset, int length) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer,offset,length);

            byte type=buf.get();
            switch(type) {
                case START:
                case GET_CONFIG:
                    return new MethodCall(type);
                case SET_OOB:
                case SET_SYNC:
                    return new MethodCall(type,buf.get() == 1);
                case SET_NUM_MSGS:
                case SET_NUM_THREADS:
                case SET_MSG_SIZE:
                    return new MethodCall(type,buf.getInt());
                case APPLY_STATE:
                    int len=buf.getInt();
                    byte[] arg=new byte[len];
                    buf.get(arg,0,arg.length);
                    return new MethodCall(type,arg);
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
        }

        private static byte[] intBuffer(short type, Integer num) {
            ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE);
            buf.put((byte)type).putInt(num);
            return buf.array();
        }

        private static byte[] longBuffer(short type, Long num) {
            ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE);
            buf.put((byte)type).putLong(num);
            return buf.array();
        }

        private static byte[] booleanBuffer(short type, Boolean arg) {
            ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE * 2);
            buf.put((byte)type).put((byte)(arg? 1 : 0));
            return buf.array();
        }
    }


    public static void main(String[] args) {
        String props=null;
        String name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        UUPerf test=null;
        try {
            test=new UUPerf();
            test.init(props,name);
            test.eventLoop();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UPerf [-props <props>] [-name name]");
    }


}