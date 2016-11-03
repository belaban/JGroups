package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.jgroups.util.Bits;

import java.io.*;
import java.lang.reflect.Field;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Dynamic tool to measure multicast performance of JGroups; every member sends N messages and we measure how long it
 * takes for all receivers to receive them.<p/>
 * All messages received from a member P are checked for ordering and non duplicity.<p/>
 * MPerf is <em>dynamic</em> because it doesn't accept any configuration
 * parameters (besides the channel config file and name); all configuration is done at runtime, and will be broadcast
 * to all cluster members.
 * @author Bela Ban (belaban@yahoo.com)
 * @since 3.1
 */
public class MPerf extends ReceiverAdapter {
    protected String                props;
    protected JChannel              channel;
    final protected AckCollector    ack_collector=new AckCollector(); // for synchronous sends
    protected Address               local_addr=null;
    protected String                name;

    protected int                   num_msgs=1000 * 1000;
    protected int                   msg_size=1000;
    protected int                   num_threads=10;
    protected int                   log_interval=num_msgs / 10; // log every 10%
    protected int                   receive_log_interval=Math.max(1, num_msgs / 10);
    protected int                   num_senders=-1; // <= 0: all
    protected boolean               oob=false;

    protected boolean cancelled=false;


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

    protected static  final NumberFormat          format=NumberFormat.getNumberInstance();
    protected static final short                  ID=ClassConfigurator.getProtocolId(MPerf.class);




    static {
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(2);
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
        channel.setReceiver(this);
        channel.connect("mperf");
        local_addr=channel.getAddress();
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "mperf", true);

        // send a CONFIG_REQ to the current coordinator, so we can get the current config
        Address coord=channel.getView().getMembers().get(0);
        if(coord != null && !local_addr.equals(coord))
            send(coord,null,MPerfHeader.CONFIG_REQ, Message.Flag.RSVP);
    }


    protected void loop() {
        int c;

        final String INPUT="[1] Send [2] View\n" +
          "[3] Set num msgs (%d) [4] Set msg size (%s) [5] Set threads (%d) [6] New config (%s)\n" +
          "[7] Number of senders (%s) [o] Toggle OOB (%s)\n" +
          "[x] Exit this [X] Exit all [c] Cancel sending";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT, num_msgs, Util.printBytes(msg_size), num_threads,
                                              props == null? "<default>" : props,
                                              num_senders <= 0? "all" : String.valueOf(num_senders), oob));
                switch(c) {
                    case '1':
                        cancelled=false;
                        initiator=true;
                        results.reset(getSenders());

                        ack_collector.reset(channel.getView().getMembers());
                        send(null,null,MPerfHeader.CLEAR_RESULTS, Message.Flag.RSVP); // clear all results (from prev runs) first
                        ack_collector.waitForAllAcks(5000);
                        
                        send(null, null, MPerfHeader.START_SENDING, Message.Flag.RSVP);
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
                        newConfig();
                        break;
                    case '7':
                        configChange("num_senders");
                        break;
                    case 'o':
                        ConfigChange change=new ConfigChange("oob", !oob);
                        send(null,change,MPerfHeader.CONFIG_CHANGE,Message.Flag.RSVP);
                        break;
                    case 'x':
                        looping=false;
                        break;
                    case 'X':
                        send(null,null,MPerfHeader.EXIT);
                        break;
                    case 'c':
                        cancelled=true;
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

        for(Result result: tmp_results.values()) {
            if(result != null) {
                total_time+=result.time;
                total_msgs+=result.msgs;
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
        send(null, change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
    }

    protected void newConfig() throws Exception {
        String filename=Util.readStringFromStdin("Config file: ");
        InputStream input=findFile(filename);
        byte[] contents=Util.readFileContents(input);
        send(null, contents, MPerfHeader.NEW_CONFIG);
        ConfigChange change=new ConfigChange("props", filename);
        send(null, change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
    }


    protected void send(Address target, Object payload, byte header, Message.Flag ... flags) throws Exception {
        Message msg=new Message(target, payload);
        if(flags != null)
            for(Message.Flag flag: flags)
                msg.setFlag(flag);
        if(header > 0)
            msg.putHeader(ID, new MPerfHeader(header));
        channel.send(msg);
    }


    protected static String printProperties() {
        StringBuilder sb=new StringBuilder();
        Properties p=System.getProperties();
        for(Iterator it=p.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }

   protected static InputStream findFile(String filename) {
        try {return new FileInputStream(filename);} catch(FileNotFoundException e) {}

        File file=new File(filename);
        String name=file.getName();
        try {return new FileInputStream(name);} catch(FileNotFoundException e) {}

        try {
            String home_dir=System.getProperty("user.home");
            filename=home_dir + File.separator + name;
            try {return new FileInputStream(filename);} catch(FileNotFoundException e) {}
        }
        catch(Throwable t) {
        }

        return Util.getResourceAsStream(name, MPerf.class);
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


    public void receive(Message msg) {
        MPerfHeader hdr=msg.getHeader(ID);
        switch(hdr.type) {
            case MPerfHeader.DATA:
                // we're checking the *application's* seqno, and multiple sender threads
                // can screw this up, that's why we check for correct order only when we
                // only have 1 sender thread
                // This is *different* from NAKACK{2} order, which is correct
                handleData(msg.getSrc(), hdr.seqno, num_threads == 1 && !oob);
                break;

            case MPerfHeader.START_SENDING:
                if(num_senders > 0) {
                    int my_rank=Util.getRank(members, local_addr);
                    if(my_rank >= 0 && my_rank > num_senders)
                        break;
                }

                result_collector=msg.getSrc();
                sendMessages();
                break;

            case MPerfHeader.SENDING_DONE:
                Address sender=msg.getSrc();
                Stats tmp=received_msgs.get(sender);
                if(tmp != null)
                    tmp.stop();

                boolean all_done=true;
                List<Address> senders=getSenders();
                for(Map.Entry<Address,Stats> entry: received_msgs.entrySet()) {
                    Address mbr=entry.getKey();
                    Stats result=entry.getValue();
                    if(!senders.contains(mbr))
                        continue;
                    if(!result.isDone()) {
                        all_done=false;
                        break;
                    }
                }

                // Compute the number messages plus time it took to receive them, for *all* members. The time is computed
                // as the time the first message was received and the time the last message was received
                if(all_done && result_collector != null) {
                    long start=0, stop=0, msgs=0;
                    for(Stats result: received_msgs.values()) {
                        if(result.start > 0)
                            start=start == 0? result.start : Math.min(start, result.start);
                        if(result.stop > 0)
                            stop=stop == 0? result.stop : Math.max(stop, result.stop);
                        msgs+=result.num_msgs_received;
                    }
                    Result result=new Result(stop-start, msgs);
                    try {
                        if(result_collector != null)
                            send(result_collector, result, MPerfHeader.RESULT, Message.Flag.RSVP);
                    }
                    catch(Exception e) {
                        System.err.println("failed sending results to " + result_collector + ": " +e);
                    }
                }
                break;

            case MPerfHeader.RESULT:
                Result res=msg.getObject();
                results.add(msg.getSrc(), res);
                if(initiator && results.hasAllResponses()) {
                    initiator=false;
                    displayResults();
                }
                break;

            case MPerfHeader.CLEAR_RESULTS:
                received_msgs.values().forEach(Stats::reset);
                total_received_msgs.set(0);
                last_interval=0;

                // requires an ACK to the sender
                try {
                    send(msg.getSrc(), null, MPerfHeader.ACK, Message.Flag.OOB);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                break;

            case MPerfHeader.CONFIG_CHANGE:
                ConfigChange config_change=msg.getObject();
                handleConfigChange(config_change);
                break;

            case MPerfHeader.CONFIG_REQ:
                try {
                    handleConfigRequest(msg.getSrc());
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
                break;

            case MPerfHeader.CONFIG_RSP:
                handleConfigResponse(msg.getObject());
                break;

            case MPerfHeader.EXIT:
                ProtocolStack stack=channel.getProtocolStack();
                String cluster_name=channel.getClusterName();
                try {
                    JmxConfigurator.unregisterChannel(channel, Util.getMBeanServer(), "jgroups", "mperf");
                }
                catch(Exception e) {
                }
                stack.stopStack(cluster_name);
                stack.destroy();
                break;

            case MPerfHeader.NEW_CONFIG:
                applyNewConfig(msg.getBuffer());
                break;

            case MPerfHeader.ACK:
                ack_collector.ack(msg.getSrc());
                break;

            default:
                System.err.println("Header type " + hdr.type + " not recognized");
        }
    }

    protected void handleData(Address src, long seqno, boolean check_order) {
        Stats result=received_msgs.get(src);
        if(result == null) {
            result=new Stats();
            Stats tmp=received_msgs.putIfAbsent(src,result);
            if(tmp != null)
                result=tmp;
        }
        result.addMessage(seqno, check_order);

        if(last_interval == 0)
            last_interval=System.currentTimeMillis();

        long received_so_far=total_received_msgs.incrementAndGet();
        if(received_so_far % receive_log_interval == 0) {
            long curr_time=System.currentTimeMillis();
            long diff=curr_time - last_interval;
            double msgs_sec=receive_log_interval / (diff / 1000.0);
            double throughput=msgs_sec * msg_size;
            last_interval=curr_time;
            System.out.println("-- received " + received_so_far + " msgs " + "(" + diff + " ms, " +
                                 format.format(msgs_sec) + " msgs/sec, " + Util.printBytes(throughput) + "/sec)");
        }
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

    protected void applyNewConfig(byte[] buffer) {
        final InputStream in=new ByteArrayInputStream(buffer);
        Thread thread=new Thread() {
            public void run() {
                try {
                    JChannel ch=new JChannel(in);
                    Util.sleepRandom(1000, 5000);
                    channel.disconnect();
                    JChannel tmp=channel;
                    channel=ch;
                    channel.setName(name);
                    channel.setReceiver(MPerf.this);
                    channel.connect("mperf");
                    local_addr=channel.getAddress();
                    JmxConfigurator.unregisterChannel(tmp, Util.getMBeanServer(), "jgroups", "mperf");
                    Util.close(tmp);
                    JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "mperf", true);
                }
                catch(Exception e) {
                    System.err.println("failed creating new channel");
                }
            }
        };

        System.out.println("<< restarting channel");
        thread.start();
    }

    protected void handleConfigChange(ConfigChange config_change) {
        String attr_name=config_change.attr_name;
        try {
            Object attr_value=config_change.getValue();
            Field field=Util.getField(this.getClass(), attr_name);
            Util.setField(field,this,attr_value);
            System.out.println(config_change.attr_name + "=" + attr_value);
            log_interval=num_msgs / 10;
            receive_log_interval=Math.max(1, num_msgs * Math.max(1, members.size()) / 10);
        }
        catch(Exception e) {
            System.err.println("failed applying config change for attr " + attr_name + ": " + e);
        }
    }

    protected void handleConfigRequest(Address sender) throws Exception {
        Configuration cfg=new Configuration();
        cfg.addChange("num_msgs",    num_msgs);
        cfg.addChange("msg_size",    msg_size);
        cfg.addChange("num_threads", num_threads);
        cfg.addChange("num_senders", num_senders);
        cfg.addChange("oob",         oob);
        send(sender,cfg,MPerfHeader.CONFIG_RSP);
    }

    protected void handleConfigResponse(Configuration cfg) {
        cfg.changes.forEach(this::handleConfigChange);
    }


    public void viewAccepted(View view) {
        System.out.println("** " + view);
        final List<Address> mbrs=view.getMembers();
        members.clear();
        members.addAll(mbrs);
        receive_log_interval=Math.max(1, num_msgs * mbrs.size() / 10);

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
        final AtomicInteger actually_sent=new AtomicInteger(0); // incremented *after* sending a message
        final AtomicLong    seqno=new AtomicLong(1); // monotonically increasing seqno, to be used by all threads
        final Sender[]      senders=new Sender[num_threads];
        final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
        final byte[]        payload=new byte[msg_size];

        for(int i=0; i < num_threads; i++) {
            senders[i]=new Sender(barrier, num_msgs_sent, actually_sent, seqno, payload);
            senders[i].setName("sender-" + i);
            senders[i].start();
        }
        try {
            System.out.println("-- sending " + num_msgs + (oob? " OOB msgs" : " msgs"));
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
        sb.append(", msgs/sec=").append(format.format(msgs_sec));
        sb.append(", throughput=").append(Util.printBytes(throughput));
        return sb.toString();
    }
    
    protected class Sender extends Thread {
        protected final CyclicBarrier barrier;
        protected final AtomicInteger num_msgs_sent, actually_sent;
        protected final AtomicLong    seqno;
        protected final byte[]        payload;

        protected Sender(CyclicBarrier barrier, AtomicInteger num_msgs_sent, AtomicInteger actually_sent,
                         AtomicLong seqno, byte[] payload) {
            this.barrier=barrier;
            this.num_msgs_sent=num_msgs_sent;
            this.actually_sent=actually_sent;
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
                    if(tmp > num_msgs || cancelled)
                        break;
                    long new_seqno=seqno.getAndIncrement();
                    Message msg=new Message(null, payload).putHeader(ID, new MPerfHeader(MPerfHeader.DATA, new_seqno));
                    if(oob)
                        msg.setFlag(Message.Flag.OOB);
                    channel.send(msg);
                    if(tmp % log_interval == 0)
                        System.out.println("++ sent " + tmp);

                    // if we used num_msgs_sent, we might have thread T3 which reaches the condition below, but
                    // actually didn't send the *last* message !
                    tmp=actually_sent.incrementAndGet(); // reuse tmp
                    if(tmp == num_msgs) // last message, send SENDING_DONE message
                        send(null, null, MPerfHeader.SENDING_DONE, Message.Flag.RSVP);
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
            Bits.writeLong(time,out);
            Bits.writeLong(msgs,out);
        }

        public void readFrom(DataInput in) throws Exception {
            time=Bits.readLong(in);
            msgs=Bits.readLong(in);
        }

        public String toString() {
            return msgs + " in " + time + " ms";
        }
    }

    protected static class MPerfHeader extends Header {
        protected static final byte DATA          =  1;
        protected static final byte START_SENDING =  2;
        protected static final byte SENDING_DONE  =  3;
        protected static final byte RESULT        =  4;
        protected static final byte CLEAR_RESULTS =  5;
        protected static final byte CONFIG_CHANGE =  6;
        protected static final byte CONFIG_REQ    =  7;
        protected static final byte CONFIG_RSP    =  8;
        protected static final byte EXIT          =  9;
        protected static final byte NEW_CONFIG    = 10;
        protected static final byte ACK           = 11;


        protected byte         type;
        protected long         seqno;

        public MPerfHeader() {}
        public MPerfHeader(byte type) {this.type=type;}
        public MPerfHeader(byte type, long seqno) {this(type); this.seqno=seqno;}
        public short getMagicId() {return 77;}
        public Supplier<? extends Header> create() {
            return MPerfHeader::new;
        }

        public int serializedSize() {
            int retval=Global.BYTE_SIZE;
            if(type == DATA)
                retval+=Bits.size(seqno);
            return retval;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            if(type == DATA)
                Bits.writeLong(seqno, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            if(type == DATA)
                seqno=Bits.readLong(in);
        }

        public String toString() {
            return typeToString(type) + " " +  (seqno > 0? seqno : "");
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case DATA:          return "DATA";
                case START_SENDING: return "START_SENDING";
                case SENDING_DONE:  return "SENDING_DONE";
                case RESULT:        return "RESULT";
                case CLEAR_RESULTS: return "CLEAR_RESULTS";
                case CONFIG_CHANGE: return "CONFIG_CHANGE";
                case CONFIG_REQ:    return "CONFIG_REQ";
                case CONFIG_RSP:    return "CONFIG_RSP";
                case EXIT:          return "EXIT";
                case NEW_CONFIG:    return "NEW_CONFIG";
                case ACK:           return "ACK";
                default:            return "n/a";
            }
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

        final MPerf test=new MPerf();
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
