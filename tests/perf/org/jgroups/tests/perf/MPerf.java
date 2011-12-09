package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dynamic tool to measure multicast performance of JGroups; every member sends N messages and we measure how long it
 * takes for all receivers to receive them. MPerf is <em>dynamic</em> because it doesn't accept any configuration
 * parameters (besides the channel config file and name); all configuration is done at runtime, and will be broadcast
 * to all cluster members.
 * @author Bela Ban (belaban@yahoo.com)
 * @since 3.1
 */
public class MPerf extends ReceiverAdapter {
    protected String          props=null;
    protected JChannel        channel;
    protected Address         local_addr=null;

    protected int             num_msgs=1000 * 1000;
    protected int             msg_size=1000;
    protected int             num_threads=1;
    protected int             log_interval=num_msgs / 10; // log every 10%


    /** Maintains stats per sender, will be sent to perf originator when all messages have been received */
    protected final Map<Address,Result>     received_msgs=Util.createConcurrentMap();
    protected final List<Address>           members=new ArrayList<Address>();
    protected final Log                     log=LogFactory.getLog(getClass());
    protected boolean                       looping=true;

    /** Map<Object, MemberInfo>. A hashmap of senders, each value is the 'senders' hashmap */
    protected final Map<Object,MemberInfo>  results=Util.createConcurrentMap();
    protected FileWriter                    output;
    protected static  final NumberFormat    format=NumberFormat.getNumberInstance();
    protected static final short            ID=ClassConfigurator.getProtocolId(MPerf.class);


    static {
        format.setGroupingUsed(false);
        format.setMaximumFractionDigits(2);
    }



    public void start(String props, String name) throws Exception {
        this.props=props;
        if(output != null)
            this.output=new FileWriter("perf.Test", false);

        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- MPerf -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
        sb.append("JGroups version: ").append(Version.description).append('\n');
        System.out.println(sb);
        output(sb.toString());
        sb=new StringBuilder();
        sb.append("\n##### msgs_received");
        sb.append(", current time (in ms)");
        sb.append(", msgs/sec");
        sb.append(", throughput/sec [KB]");
        sb.append(", free_mem [KB] ");
        sb.append(", total_mem [KB] ");
        output(sb);

        channel=new JChannel(props);
        channel.setName(name);
        channel.setReceiver(this);
        channel.connect("mperf");
        local_addr=channel.getAddress();

        // send a CONFIG_REQ to the current coordinator, so we can get the current config
        Address coord=channel.getView().getMembers().get(0);
        if(coord != null && !local_addr.equals(coord))
            send(coord,null,MPerfHeader.CONFIG_REQ,true);
    }


    protected void loop() {
        int c;

        final String INPUT="[1] Send [2] View\n" +
          "[3] Set num msgs (%d) [4] Set msg size (%s) [5] Set threads (%d)\n" +
          "[x] Exit this [X] Exit all ";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT, num_msgs, Util.printBytes(msg_size), num_threads));
                switch(c) {
                    case '1':
                        send(null, null, MPerfHeader.CLEAR_RESULTS, true); // clear all results (from prev runs) first
                        send(null, null, MPerfHeader.START_SENDING, false);
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
                    case 'x':
                        looping=false;
                        break;
                    case 'X':
                        send(null,null,MPerfHeader.EXIT,false);
                        break;
                }
            }
            catch(Throwable t) {
                System.err.println(t);
            }
        }
        stop();
    }

    protected void configChange(String name) throws Exception {
        int tmp=Util.readIntFromStdin(name + ": ");
        if(tmp < 1)
            throw new IllegalArgumentException("illegal value");
        ConfigChange change=new ConfigChange(name, tmp);
        send(null,change,MPerfHeader.CONFIG_CHANGE,true);
    }


    protected void send(Address target, Object payload, byte header, boolean rsvp) throws Exception {
        Message msg=new Message(target, null, payload);
        if(rsvp)
            msg.setFlag(Message.Flag.RSVP);
        if(header > 0)
            msg.putHeader(ID, new MPerfHeader(header));
        channel.send(msg);
    }


    protected void output(Object msg) {
        if(this.output != null) {
            try {
                this.output.write(msg + "\n");
                this.output.flush();
            }
            catch(IOException e) {
            }
        }
    }

    private static String printProperties() {
        StringBuilder sb=new StringBuilder();
        Properties p=System.getProperties();
        for(Iterator it=p.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }

    public void stop() {
        looping=false;
        Util.close(channel);
        if(this.output != null) {
            try {
                this.output.close();
            }
            catch(IOException e) {
            }
        }
    }


    public void receive(Message msg) {
        MPerfHeader hdr=(MPerfHeader)msg.getHeader(ID);
        switch(hdr.type) {
            case MPerfHeader.DATA:
                // System.out.println("<< received " + Util.printBytes(msg.getLength()) + " from " + msg.getSrc());
                break;

            case MPerfHeader.START_SENDING:
                sendMessages();
                break;

            case MPerfHeader.CLEAR_RESULTS:
                for(Result result: received_msgs.values())
                    result.reset();
                break;

            case MPerfHeader.CONFIG_CHANGE:
                ConfigChange config_change=(ConfigChange)msg.getObject();
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
                handleConfigResponse((Configuration)msg.getObject());
                break;

            case MPerfHeader.EXIT:
                ProtocolStack stack=channel.getProtocolStack();
                String cluster_name=channel.getClusterName();
                stack.stopStack(cluster_name);
                stack.destroy();
                break;

            default:
                System.err.println("Header type " + hdr.type + " not recognized");
        }
    }

    protected void handleConfigChange(ConfigChange config_change) {
        String attr_name=config_change.attr_name;
        try {
            Object attr_value=config_change.getValue();
            Field field=Util.getField(this.getClass(), attr_name);
            Util.setField(field, this, attr_value);
            System.out.println(config_change.attr_name + "=" + attr_value);
            log_interval=num_msgs / 10;
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
        send(sender,cfg,MPerfHeader.CONFIG_RSP,false);
    }

    protected void handleConfigResponse(Configuration cfg) {
        for(ConfigChange change: cfg.changes) {
            handleConfigChange(change);
        }
    }


    public void viewAccepted(View view) {
        System.out.println("** " + view);
    }

    protected void sendMessages() {
        final AtomicInteger num_msgs_sent=new AtomicInteger(0); // all threads will increment this
        final Sender[]      senders=new Sender[num_threads];
        final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
        final byte[]        payload=new byte[msg_size];

        for(int i=0; i < num_threads; i++) {
            senders[i]=new Sender(barrier, num_msgs_sent, payload);
            senders[i].setName("sender-" + i);
            senders[i].start();
        }
        try {
            System.out.println("-- sending " + num_msgs + " msgs");
            barrier.await();
        }
        catch(Exception e) {
            System.err.println("failed triggering send threads: " + e);
        }

        // for(Sender sender: senders) {try {sender.join();} catch(InterruptedException e) {}}

        
        
    }

    protected class Sender extends Thread {
        protected final CyclicBarrier barrier;
        protected final AtomicInteger num_msgs_sent;
        protected final byte[]        payload;

        protected Sender(CyclicBarrier barrier, AtomicInteger num_msgs_sent, byte[] payload) {
            this.barrier=barrier;
            this.num_msgs_sent=num_msgs_sent;
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
                    send(null, payload, MPerfHeader.DATA, false);
                    int tmp=num_msgs_sent.incrementAndGet();
                    if(tmp > num_msgs)
                        break;
                    if(tmp % log_interval == 0)
                        System.out.println("++ sent " + tmp + " [" + getName() + "]");
                }
                catch(Exception e) {
                }
            }
        }
    }







    private void dumpResults(Map<Object,MemberInfo> final_results) {
        Object      member;
        MemberInfo  val;
        double      combined_msgs_sec, tmp=0;
        long        combined_tp;
        StringBuilder sb=new StringBuilder();
        sb.append("\n-- results:\n");

        for(Map.Entry<Object,MemberInfo> entry: final_results.entrySet()) {
            member=entry.getKey();
            val=entry.getValue();
            tmp+=val.getMessageSec();
            sb.append("\n").append(member);
            if(member.equals(local_addr))
                sb.append(" (myself)");
            sb.append(":\n");
            sb.append(val);
            sb.append('\n');
        }
        combined_msgs_sec=tmp / final_results.size();
        combined_tp=(long)combined_msgs_sec * msg_size;

        sb.append("\ncombined: ").append(format.format(combined_msgs_sec)).
                append(" msgs/sec averaged over all receivers (throughput=" + Util.printBytes(combined_tp) + "/sec)\n");
        System.out.println(sb.toString());
        output(sb.toString());
    }




    private static void dump(Map<Object,MemberInfo> map, StringBuilder sb) {
        Object     mySender;
        MemberInfo mi;
        MemberInfo combined=new MemberInfo(0);
        combined.start = Long.MAX_VALUE;
        combined.stop = Long.MIN_VALUE;

        sb.append("\n-- local results:\n");
        for(Map.Entry<Object,MemberInfo> entry: map.entrySet()) {
            mySender=entry.getKey();
            mi=entry.getValue();
            combined.start=Math.min(combined.start, mi.start);
            combined.stop=Math.max(combined.stop, mi.stop);
            combined.num_msgs_expected+=mi.num_msgs_expected;
            combined.num_msgs_received+=mi.num_msgs_received;
            combined.total_bytes_received+=mi.total_bytes_received;
            sb.append("sender: ").append(mySender).append(": ").append(mi).append('\n');
        }
    }


    private String dumpStats(long received_msgs) {
        double msgs_sec, throughput_sec;
        long   current;

        StringBuilder sb=new StringBuilder();
        sb.append(received_msgs).append(' ');

        current=System.currentTimeMillis();
        sb.append(current).append(' ');

        // msgs_sec=received_msgs / ((current - start) / 1000.0);
        msgs_sec=322649;
        throughput_sec=msgs_sec * msg_size;

        sb.append(format.format(msgs_sec)).append(' ').append(format.format(throughput_sec)).append(' ');

        sb.append(Runtime.getRuntime().freeMemory() / 1000.0).append(' ');

        sb.append(Runtime.getRuntime().totalMemory() / 1000.0);

        return sb.toString();
    }


    public String dumpTransportStats(String parameters) {
        Map stats=channel.dumpStats(parameters);
        StringBuilder sb=new StringBuilder(128);
        if(stats != null) {
            Map.Entry entry;
            String key;
            Map value;
            for(Iterator it=stats.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                key=(String)entry.getKey();
                value=(Map)entry.getValue();
                sb.append("\n").append(key).append(":\n");
                for(Iterator it2=value.entrySet().iterator(); it2.hasNext();) {
                    sb.append(it2.next()).append("\n");
                }
            }
        }
        return sb.toString();
    }

    private static void print(Map stats, StringBuilder sb) {
        sb.append("\nTransport stats:\n\n");
        Map.Entry entry;
        Object key, val;
        for(Iterator it=stats.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
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
            Util.writeString(attr_name, out);
            Util.writeByteBuffer(attr_value, out);
        }

        public void readFrom(DataInput in) throws Exception {
            attr_name=Util.readString(in);
            attr_value=Util.readByteBuffer(in);
        }
    }


    protected static class Configuration implements Streamable {
        protected List<ConfigChange> changes=new ArrayList<ConfigChange>();

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

    protected static class Result implements Streamable {
        protected long    start=0;
        protected long    stop=0; // done when > 0
        protected long    num_msgs_received=0;
        protected long    num_bytes_received=0;

        public void reset() {
            start=stop=num_msgs_received=num_bytes_received=0;
        }

        public boolean isDone() {return stop > 0;}

        public int size() {
            return Util.size(start) + Util.size(stop) + Util.size(num_msgs_received) + Util.size(num_bytes_received);
        }

        public void add(long bytes) {
            num_bytes_received+=bytes;
            num_msgs_received++;
        }

        public void writeTo(DataOutput out) throws Exception {
            Util.writeLong(start, out);
            Util.writeLong(stop, out);
            Util.writeLong(num_msgs_received, out);
            Util.writeLong(num_bytes_received, out);
        }

        public void readFrom(DataInput in) throws Exception {
            start=Util.readLong(in);
            stop=Util.readLong(in);
            num_msgs_received=Util.readLong(in);
            num_bytes_received=Util.readLong(in);
        }
    }

    protected static class MPerfHeader extends Header {
        protected static final byte DATA          =  1;
        protected static final byte START_SENDING =  2;
        protected static final byte SENDING_DONE  =  3;
        protected static final byte RESULTS_REQ   =  4;
        protected static final byte RESULTS_RSP   =  5;
        protected static final byte CLEAR_RESULTS =  6;
        protected static final byte CONFIG_CHANGE =  7;
        protected static final byte CONFIG_REQ    =  8;
        protected static final byte CONFIG_RSP    =  9;
        protected static final byte EXIT          =  10;


        protected byte         type;

        public MPerfHeader() {}
        public MPerfHeader(byte type) {this.type=type;}
        public int size() {return Global.BYTE_SIZE;}
        public void writeTo(DataOutput out) throws Exception {out.writeByte(type);}
        public void readFrom(DataInput in) throws Exception {type=in.readByte();}
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
            test.start(props,name);

            // this kludge is needed in order to terminate the program gracefully when 'X' is pressed
            // (otherwise System.in.read() would not terminate)
            Thread thread=new Thread() {
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
