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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
    protected int             warmup=200000;
    protected int             msg_size=1000;
    protected int             num_threads=1;
    protected int             log_interval=num_msgs / 10; // log every 10%


    /** Maintains stats per sender, will be sent to perf originator when all messages have been received */
    protected final Map<Address,MemberInfo> senders=Util.createConcurrentMap();
    protected final List<Address>           members=new ArrayList<Address>();
    protected final Log                     log=LogFactory.getLog(getClass());
    protected boolean                       looping=true;

    /** Map<Object, MemberInfo>. A hashmap of senders, each value is the 'senders' hashmap */
    protected final Map<Object,MemberInfo>  results=Util.createConcurrentMap();
    protected FileWriter                    output;
    protected static  final NumberFormat    f=NumberFormat.getNumberInstance();
    protected static final short            ID=ClassConfigurator.getProtocolId(MPerf.class);


    static {
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
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
            sendConfigRequest(coord);
    }


    protected void loop() {
        int c;

        final String INPUT="[1] Send [2] View\n" +
          "[3] Set num msgs (%d) [4] Set warmup (%d) [5] Set msg size (%s) [6] Set threads (%d)\n" +
          "[x] Exit this [X] Exit all ";

        while(looping) {
            try {
                c=Util.keyPress(String.format(INPUT, num_msgs,warmup, Util.printBytes(msg_size), num_threads));
                switch(c) {
                    case '1':
                        break;
                    case '2':
                        System.out.println("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
                        break;
                    case '3':
                        configChange("num_msgs");
                        break;
                    case '4':
                        configChange("warmup");
                        break;
                    case '5':
                        configChange("msg_size");
                        break;
                    case '6':
                        configChange("num_threads");
                        break;
                    case 'x':
                        looping=false;
                        break;
                    case 'X':
                        sendExit();
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
        ConfigChange change=new ConfigChange(name, tmp);
        Message msg=new Message(null, null, change);
        msg.setFlag(Message.Flag.RSVP);
        msg.putHeader(ID,new MPerfHeader(MPerfHeader.CONFIG_CHANGE));
        channel.send(msg);
    }

    protected void sendExit() throws Exception {
        Message msg=new Message();
        msg.putHeader(ID, new MPerfHeader(MPerfHeader.EXIT));
        channel.send(msg);
    }

    // Send a CONFIG_REQ to the current coordinator
    protected void sendConfigRequest(Address coord) throws Exception {
        Message msg=new Message(coord);
        msg.setFlag(Message.Flag.RSVP);
        msg.putHeader(ID, new MPerfHeader(MPerfHeader.CONFIG_REQ));
        channel.send(msg);
    }

    protected void sendConfigurationResponse(Address target, Configuration cfg) throws Exception {
        Message msg=new Message(target, null, cfg);
        msg.putHeader(ID, new MPerfHeader(MPerfHeader.CONFIG_RSP));
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
        cfg.addChange("warmup",      warmup);
        cfg.addChange("msg_size",    msg_size);
        cfg.addChange("num_threads", num_threads);
        sendConfigurationResponse(sender, cfg);
    }

    protected void handleConfigResponse(Configuration cfg) {
        for(ConfigChange change: cfg.changes) {
            handleConfigChange(change);
        }
    }


    public void viewAccepted(View view) {
        System.out.println("** " + view);
    }

    protected class Sender extends Thread {
        CyclicBarrier barrier;
        byte[] payload;
        long total_msgs=0;
        long thread_interval;

        Sender(CyclicBarrier barrier, byte[] payload, long thread_interval) {
            this.barrier=barrier;
            this.payload=payload;
            this.thread_interval=thread_interval;
        }

        public void run() {
            try {
                barrier.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }
            catch(BrokenBarrierException e) {
                e.printStackTrace();
                return;
            }
            System.out.println("-- [" + getName() + "] sending " + num_msgs + " msgs");
            for(int i=0; i < num_msgs; i++) {
                try {
                    channel.send(null, payload);
                    total_msgs++;
                    if(total_msgs % log_interval == 0) {
                        System.out.println("++ sent " + total_msgs + " [" + getName() + "]");
                    }
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

        sb.append("\ncombined: ").append(f.format(combined_msgs_sec)).
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

        sb.append(f.format(msgs_sec)).append(' ').append(f.format(throughput_sec)).append(' ');

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

    protected static class MPerfHeader extends Header {
        protected static final byte DATA          = 1;
        protected static final byte START_SENDING = 2;
        protected static final byte SENDING_DONE  = 3;
        protected static final byte RESULTS_REQ   = 4;
        protected static final byte RESULTS_RSP   = 5;
        protected static final byte CONFIG_CHANGE = 6;
        protected static final byte CONFIG_REQ    = 7;
        protected static final byte CONFIG_RSP    = 8;
        protected static final byte EXIT          = 9;

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
