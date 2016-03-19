package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dynamic tool to measure multicast RPC performance of JGroups; every member invokes N RPCs and we measure how long it
 * takes for all receivers to receive them.<p/>
 * Initially copied from {@link MPerf}.
 * @author Bela Ban (belaban@yahoo.com)
 * @since 3.3
 */
public class MPerfRpc extends ReceiverAdapter {
    protected String                              props;
    protected JChannel                            channel;
    protected RpcDispatcher                       disp;
    protected Address                             local_addr;
    protected String                              name;

    protected int                                 num_msgs=1000 * 1000;
    protected int                                 msg_size=1000;
    protected int                                 num_threads=25;
    protected int                                 log_interval=num_msgs / 10; // log every 10%
    protected int                                 receive_log_interval=num_msgs / 10;
    protected int                                 num_senders=-1; // <= 0: all
    protected boolean                             sync=true; // sync RPCs by default
    protected boolean                             oob=false; // mark messages as OOB


    /** Maintains stats per sender, will be sent to perf originator when all messages have been received */
    protected final ConcurrentMap<Address,Stats>  received_msgs=Util.createConcurrentMap();
    protected final AtomicLong                    total_received_msgs=new AtomicLong(0);
    protected final List<Address>                 members=new CopyOnWriteArrayList<>();
    protected final Log                           log=LogFactory.getLog(getClass());
    protected boolean                             looping=true;
    protected long                                last_interval=0;
    protected final ResponseCollector<Result>     results=new ResponseCollector<>();

    // the member which will collect and display the overall results
    protected volatile Address                    result_collector=null;
    protected volatile boolean                    initiator=false;
    protected RequestOptions                      send_options=RequestOptions.ASYNC();

    protected static  final NumberFormat          format=NumberFormat.getNumberInstance();

    // Callbacks invoked
    protected static final short                  handleData   =  0;
    protected static final short                  startSending =  1;
    protected static final short                  sendingDone  =  2;
    protected static final short                  result       =  3;
    protected static final short                  clearResults =  4;
    protected static final short                  configChange =  5;
    protected static final short                  configReq    =  6;
    protected static final short                  configRsp    =  7;
    protected static final short                  exit         =  8;

    protected static final Method[]               METHODS=new Method[9];




    static {
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(2);
        try {
            METHODS[handleData]=MPerfRpc.class.getMethod("handleData", Address.class, byte[].class, long.class, boolean.class);
            METHODS[startSending]=MPerfRpc.class.getMethod("startSending", Address.class);
            METHODS[sendingDone]=MPerfRpc.class.getMethod("sendingDone", Address.class);
            METHODS[result]=MPerfRpc.class.getMethod("result", Address.class, Result.class);
            METHODS[clearResults]=MPerfRpc.class.getMethod("clearResults");
            METHODS[configChange]=MPerfRpc.class.getMethod("configChange", ConfigChange.class);
            METHODS[configReq]=MPerfRpc.class.getMethod("configReq", Address.class);
            METHODS[configRsp]=MPerfRpc.class.getMethod("configRsp", Configuration.class);
            METHODS[exit]=MPerfRpc.class.getMethod("exit");
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }



    public void start(String props, String name) throws Exception {
        this.props=props;
        this.name=name;
        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- MPerf -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
        sb.append("JGroups version: ").append(Version.description).append('\n');
        System.out.println(sb);

        channel=new JChannel(props);
        channel.setName(name);
        disp=new RpcDispatcher(channel, this).setMembershipListener(this).setMethodLookup(id -> METHODS[id])
          .setMarshaller(new MperfMarshaller());

        send_options.mode(sync? ResponseMode.GET_ALL : ResponseMode.GET_NONE);
        if(oob)
            send_options.flags(Message.Flag.OOB);

        channel.connect("mperf");
        local_addr=channel.getAddress();
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "mperf", true);

        // send a CONFIG_REQ to the current coordinator, so we can get the current config
        Address coord=channel.getView().getMembers().get(0);
        if(coord != null && !local_addr.equals(coord))
            invokeRpc(configReq, coord, RequestOptions.ASYNC().flags(Message.Flag.RSVP), local_addr);
    }


    protected void loop() {
        int c;

        final String INPUT="[1] Invoke [2] View\n" +
          "[3] Set num RPCs (%d) [4] Set msg size (%s) [5] Set threads (%d)\n" +
          "[6] Number of senders (%s) [s] Toggle sync (%s) [o] Toggle OOB (%s)\n" +
          "[x] Exit this [X] Exit all";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT, num_msgs, Util.printBytes(msg_size), num_threads,
                                              num_senders <= 0? "all" : String.valueOf(num_senders),
                                              sync, oob));
                switch(c) {
                    case '1':
                        initiator=true;
                        results.reset(getSenders());
                        invokeRpc(clearResults,RequestOptions.SYNC().flags(Message.Flag.RSVP));
                        invokeRpc(startSending, RequestOptions.ASYNC(), local_addr);
                        break;
                    case '2':
                        System.out.println("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
                        break;
                    case '3':
                        configChange("num_msgs");
                        break;
                    case '4':
                        configChange("msg_size");
                        break;
                    case '5':
                        configChange("num_threads");
                        break;
                    case '6':
                        configChange("num_senders");
                        break;
                    case 's':
                        ConfigChange change=new ConfigChange("sync", !sync);
                        invokeRpc(configChange, RequestOptions.SYNC().flags(Message.Flag.RSVP), change);
                        break;
                    case 'o':
                        change=new ConfigChange("oob", !oob);
                        invokeRpc(configChange, RequestOptions.SYNC().flags(Message.Flag.RSVP), change);
                        break;
                    case 'x':
                        looping=false;
                        break;
                    case 'X':
                        invokeRpc(exit,RequestOptions.ASYNC());
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        stop();
    }


    protected void displayResults() {
        System.out.println("\nResults:\n");
        Map<Address,Result> tmp_results=results.getResults();
        for(Map.Entry<Address,Result> entry: tmp_results.entrySet()) {
            Result val=entry.getValue();
            if(val != null)
                System.out.println(entry.getKey() + ": " + computeStats(val.time, val.msgs, msg_size));
        }

        long total_msgs=0, total_time=0, num=0;
        for(Result res: tmp_results.values()) {
            if(res != null) {
                total_time+=res.time;
                total_msgs+=res.msgs;
                num++;
            }
        }
        if(num > 0) {
            System.out.println("\n===============================================================================");
            System.out.println("\033[1m Average/node:    " + computeStats(total_time / num, total_msgs / num, msg_size));
            System.out.println("\033[0m Average/cluster: " + computeStats(total_time/num, total_msgs, msg_size));
            System.out.println("================================================================================\n\n");
        }
        else {
            System.out.println("\n===============================================================================");
            System.out.println("\033[1m Received no results");
            System.out.println("\033[0m");
            System.out.println("================================================================================\n\n");
        }
    }

    protected void configChange(String name) throws Exception {
        int tmp=Util.readIntFromStdin(name + ": ");
        ConfigChange change=new ConfigChange(name, tmp);
        invokeRpc(configChange, RequestOptions.SYNC().flags(Message.Flag.RSVP), change);
    }



    protected RspList invokeRpc(short method_id, RequestOptions options, Object ... args) throws Exception {
        MethodCall call=new MethodCall(method_id, args);
        return disp.callRemoteMethods(null, call, options);
    }

    protected Object invokeRpc(short method_id, Address dest, RequestOptions options, Object ... args) throws Exception {
        MethodCall call=new MethodCall(method_id, args);
        return disp.callRemoteMethod(dest, call, options);
    }



    public void stop() {
        looping=false;
        try {
            JmxConfigurator.unregisterChannel(channel, Util.getMBeanServer(), "jgroups", "mperf");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        Util.close(channel);
    }




    // we're checking the *application's* seqno, and multiple sender threads can screw this up,
    // that's why we check for correct order only when we only have 1 sender thread
    // This is *different* from NAKACK{2} order, which is correct
    public void handleData(Address src, byte[] payload, long seqno, boolean check_order) { // 0
        int length=payload.length;
        if(length == 0)
            return;
        Stats tmp_result=received_msgs.get(src);
        if(tmp_result == null) {
            tmp_result=new Stats();
            Stats tmp=received_msgs.putIfAbsent(src,tmp_result);
            if(tmp != null)
                tmp_result=tmp;
        }
        tmp_result.addMessage(seqno,check_order);

        if(last_interval == 0)
            last_interval=System.currentTimeMillis();

        long received_so_far=total_received_msgs.incrementAndGet();
        if(received_so_far % receive_log_interval == 0) {
            long curr_time=System.currentTimeMillis();
            long diff=curr_time - last_interval;
            double msgs_sec=receive_log_interval / (diff / 1000.0);
            double throughput=msgs_sec * msg_size;
            last_interval=curr_time;
            System.out.println("-- received " + received_so_far + " rpcs " + "(" + diff + " ms, " +
                                 format.format(msgs_sec) + " rpcs/sec, " + Util.printBytes(throughput) + "/sec)");
        }
    }

    public void startSending(Address initiator) { // 1
        if(num_senders > 0) {
            int my_rank=Util.getRank(members, local_addr);
            if(my_rank >= 0 && my_rank > num_senders)
                return;
        }

        result_collector=initiator;
        sendMessages();
    }

    public void sendingDone(Address sender) { // 2
        Stats tmp=received_msgs.get(sender);
        if(tmp != null)
            tmp.stop();

        boolean all_done=true;
        List<Address> senders=getSenders();
        for(Map.Entry<Address,Stats> entry: received_msgs.entrySet()) {
            Address mbr=entry.getKey();
            Stats tmp_result=entry.getValue();
            if(!senders.contains(mbr))
                continue;
            if(!tmp_result.isDone()) {
                all_done=false;
                break;
            }
        }

        // Compute the number messages plus time it took to receive them, for *all* members. The time is computed
        // as the time the first message was received and the time the last message was received
        if(all_done && result_collector != null) {
            long start=0, stop=0, msgs=0;
            for(Stats res: received_msgs.values()) {
                if(res.start > 0)
                    start=start == 0? res.start : Math.min(start, res.start);
                if(res.stop > 0)
                    stop=stop == 0? res.stop : Math.max(stop, res.stop);
                msgs+=res.num_msgs_received;
            }
            Result tmp_result=new Result(stop-start, msgs);
            try {
                if(result_collector != null)
                    invokeRpc(result, result_collector, RequestOptions.SYNC().flags(Message.Flag.RSVP),
                              local_addr, tmp_result);
            }
            catch(Exception e) {
                System.err.println("failed sending results to " + result_collector + ": " +e);
            }
        }

    }

    /** Called when a result from a node is received */
    public void result(Address sender, Result res) { // 3
        results.add(sender, res);
        if(initiator && results.hasAllResponses()) {
            initiator=false;
            displayResults();
        }
    }

    public void clearResults() { // 4
        received_msgs.values().forEach(Stats::reset);
        total_received_msgs.set(0);
        last_interval=0;
    }

    public void configChange(ConfigChange config_change) { // 5
        String attr_name=config_change.attr_name;
        try {
            Object attr_value=config_change.getValue();
            Field field=Util.getField(this.getClass(), attr_name);
            Util.setField(field, this, attr_value);
            System.out.println(config_change.attr_name + "=" + attr_value);
            log_interval=num_msgs / 10;
            receive_log_interval=num_msgs * Math.max(1, members.size()) / 10;
            send_options.mode(sync? ResponseMode.GET_ALL : ResponseMode.GET_NONE);
            if(oob)
                send_options.flags(Message.Flag.OOB);
        }
        catch(Exception e) {
            System.err.println("failed applying config change for attr " + attr_name + ": " + e);
        }
    }

    public void configReq(Address sender) throws Exception { // 6
        Configuration cfg=new Configuration();
        cfg.addChange("num_msgs",    num_msgs);
        cfg.addChange("msg_size",    msg_size);
        cfg.addChange("num_threads", num_threads);
        cfg.addChange("num_senders", num_senders);
        cfg.addChange("sync",        sync);
        cfg.addChange("oob",         oob);
        invokeRpc(configRsp, sender, RequestOptions.ASYNC(), cfg);
    }

    public void configRsp(Configuration cfg) { // 7
        cfg.changes.forEach(this::configChange);
    }

    public void exit() { // 8
        ProtocolStack stack=channel.getProtocolStack();
        String cluster_name=channel.getClusterName();
        try {
            JmxConfigurator.unregisterChannel(channel, Util.getMBeanServer(), "jgroups", "mperf");
        }
        catch(Exception e) {
        }
        stack.stopStack(cluster_name);
        stack.destroy();
    }





    /** Returns all members if num_senders <= 0, or the members with rank <= num_senders */
    protected List<Address> getSenders() {
        if(num_senders <= 0)
            return new ArrayList<>(members);
        List<Address> retval=new ArrayList<>();
        for(int i=0; i < num_senders; i++)
            retval.add(members.get(i));
        return retval;
    }



    public void viewAccepted(View view) {
        System.out.println("** " + view);
        final List<Address> mbrs=view.getMembers();
        members.clear();
        members.addAll(mbrs);
        receive_log_interval=num_msgs * mbrs.size() / 10;

        // Remove non members from received messages
        received_msgs.keySet().retainAll(mbrs);

        // Add non-existing elements
        for(Address member: mbrs)
            received_msgs.putIfAbsent(member, new Stats());

        results.retainAll(mbrs);

        if(result_collector != null && !mbrs.contains(result_collector))
            result_collector=null;
    }

    
    protected void sendMessages() {
        final AtomicInteger num_msgs_sent=new AtomicInteger(0); // all threads will increment this
        final AtomicLong    seqno=new AtomicLong(1); // monotonically increasing seqno, to be used by all threads
        final Sender[]      senders=new Sender[num_threads];
        final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
        final byte[]        payload=new byte[msg_size];

        for(int i=0; i < num_threads; i++) {
            senders[i]=new Sender(barrier, num_msgs_sent, seqno, payload);
            senders[i].setName("invoker-" + i);
            senders[i].start();
        }
        try {
            System.out.println("-- invoking " + num_msgs + " msgs");
            barrier.await();
        }
        catch(Exception e) {
            System.err.println("failed triggering send threads: " + e);
        }
    }


    protected static String computeStats(long time, long msgs, int size) {
        StringBuilder sb=new StringBuilder();
        double msgs_sec, throughput=0;
        msgs_sec=msgs / (time/1000.0);
        throughput=(msgs * size) / (time / 1000.0);
        sb.append(msgs).append(" msgs, ");
        sb.append(Util.printBytes(msgs * size)).append(" received");
        sb.append(", time=").append(format.format(time)).append("ms");
        sb.append(", rpcs/sec=").append(format.format(msgs_sec));
        sb.append(", throughput=").append(Util.printBytes(throughput));
        return sb.toString();
    }
    
    protected class Sender extends Thread {
        protected final CyclicBarrier barrier;
        protected final AtomicInteger num_msgs_sent;
        protected final AtomicLong    seqno;
        protected final byte[]        payload;

        protected Sender(CyclicBarrier barrier, AtomicInteger num_msgs_sent, AtomicLong seqno, byte[] payload) {
            this.barrier=barrier;
            this.num_msgs_sent=num_msgs_sent;
            this.seqno=seqno;
            this.payload=payload;
        }

        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
                return;
            }

            for(;;) {
                try {
                    int tmp=num_msgs_sent.incrementAndGet();
                    if(tmp > num_msgs)
                        break;
                    long new_seqno=seqno.getAndIncrement();
                    invokeRpc(handleData, send_options, local_addr, payload, new_seqno, num_threads == 1);
                    if(tmp % log_interval == 0)
                        System.out.println("++ sent " + tmp);
                    if(tmp == num_msgs) { // last message, send SENDING_DONE message
                        RequestOptions opts=RequestOptions.ASYNC().flags(Message.Flag.RSVP);
                        invokeRpc(sendingDone, opts, local_addr);
                    }
                }
                catch(Exception e) {
                }
            }
        }
    }



    protected static class ConfigChange implements Streamable {
        protected String attr_name;
        protected byte[] attr_value;


        public ConfigChange() {
        }

        public ConfigChange(String attr_name, Object val) throws Exception {
            this.attr_name=attr_name;
            this.attr_value=Util.objectToByteBuffer(val);
        }

        public Object getValue() throws Exception {
            return Util.objectFromByteBuffer(attr_value);
        }

        public int size() {
            return Util.size(attr_name) + Util.size(attr_value);
        }

        public void writeTo(DataOutput out) throws Exception {
            Bits.writeString(attr_name,out);
            Util.writeByteBuffer(attr_value, out);
        }

        public void readFrom(DataInput in) throws Exception {
            attr_name=Bits.readString(in);
            attr_value=Util.readByteBuffer(in);
        }
    }


    protected static class Configuration implements Streamable {
        protected List<ConfigChange> changes=new ArrayList<>();

        public Configuration() {
        }

        public Configuration addChange(String key, Object val) throws Exception {
            if(key != null && val != null) {
                changes.add(new ConfigChange(key, val));
            }
            return this;
        }

        public int size() {
            int retval=Global.INT_SIZE;
            for(ConfigChange change: changes)
                retval+=change.size();
            return retval;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeInt(changes.size());
            for(ConfigChange change: changes)
                change.writeTo(out);
        }

        public void readFrom(DataInput in) throws Exception {
            int len=in.readInt();
            for(int i=0; i < len; i++) {
                ConfigChange change=new ConfigChange();
                change.readFrom(in);
                changes.add(change);
            }
        }
    }

    protected class Stats {
        protected long    start=0;
        protected long    stop=0; // done when > 0
        protected long    num_msgs_received=0;
        protected long    seqno=1; // next expected seqno

        public void reset() {
            start=stop=num_msgs_received=0;
            seqno=1;
        }

        public void    stop() {stop=System.currentTimeMillis();}
        public boolean isDone() {return stop > 0;}

        /**
         * Adds the message and checks whether the messages are received in FIFO order. If we have multiple threads
         * (check_order=false), then this check canot be performed
         * @param seqno
         * @param check_order
         */
        public void addMessage(long seqno, boolean check_order) {
            if(start == 0)
                start=System.currentTimeMillis();
            if(seqno != this.seqno && check_order)
                throw new IllegalStateException("expected seqno=" + this.seqno + ", but received " + seqno);
            this.seqno++;
            num_msgs_received++;
        }

        public String toString() {
            return computeStats(stop - start,num_msgs_received,msg_size);
        }
    }

    protected static class Result implements Streamable {
        protected long    time=0;
        protected long    msgs=0;

        public Result() {
        }

        public Result(long time, long msgs) {
            this.time=time;
            this.msgs=msgs;
        }

        public int size() {
            return Bits.size(time) + Bits.size(msgs);
        }

        public void writeTo(DataOutput out) throws Exception {
            Bits.writeLong(time, out);
            Bits.writeLong(msgs, out);
        }

        public void readFrom(DataInput in) throws Exception {
            time=Bits.readLong(in);
            msgs=Bits.readLong(in);
        }

        public String toString() {
            return msgs + " in " + time + " ms";
        }
    }

    protected class MperfMarshaller implements Marshaller {

        public int estimatedSize(Object arg) {
            if(arg == null) return 10;
            if(arg instanceof byte[])
                return MPerfRpc.this.msg_size;
            return 50;
        }

        public void objectToStream(Object obj, DataOutput out) throws Exception {
            Util.objectToStream(obj, out);
        }

        public Object objectFromStream(DataInput in) throws Exception {
            return Util.objectFromStream(in);
        }

    }


    public static void main(String[] args) {
        String props=null, name=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            System.out.println("MPerf [-props <stack config>] [-name <logical name>]");
            return;
        }

        final MPerfRpc test=new MPerfRpc();
        try {
            test.start(props, name);

            // this kludge is needed in order to terminate the program gracefully when 'X' is pressed
            // (otherwise System.in.read() would not terminate)
            Thread thread=new Thread("MPerf runner") {
                public void run() {
                    test.loop();
                }
            };
            thread.setDaemon(true);
            thread.start();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
