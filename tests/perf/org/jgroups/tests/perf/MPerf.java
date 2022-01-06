package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Dynamic tool to measure multicast performance of JGroups; every member sends N messages and we measure how long it
 * takes for all receivers to receive them.<p/>
 * All messages received from a member P are checked for ordering and non duplicity.<p/>
 * MPerf is <em>dynamic</em> because it doesn't accept any configuration
 * parameters (besides the channel config file and name); all configuration is done at runtime, and will be broadcast
 * to all cluster members.
 * @author Bela Ban (belaban@gmail.com)
 * @since 3.1
 */
public class MPerf implements Receiver {
    protected JChannel                        channel;
    protected Address                         local_addr;
    protected int                             time=60; // seconds
    protected int                             msg_size=1000;
    protected int                             num_threads=100;
    protected int                             num_senders=-1; // <= 0: all
    protected boolean                         oob;
    protected boolean                         log_local=true; // default: same behavior as before
    protected boolean                         display_msg_src=false;
    protected long                            start_time; // set on reception of START message
    protected MessageCounter                  received_msgs_map=new MessageCounter();
    protected final List<Address>             members=new CopyOnWriteArrayList<>();
    protected final Log                       log=LogFactory.getLog(getClass());
    protected Path                            out_file_path;
    protected boolean                         looping=true;
    protected final ResponseCollector<Result> results=new ResponseCollector<>();
    protected ThreadFactory                   thread_factory;
    protected static final short              ID=ClassConfigurator.getProtocolId(MPerf.class);

    public MPerf() throws IOException {
        this(null);
    }

    public MPerf(Path out_file_path) throws IOException {
        if (out_file_path != null) {
            if (Files.notExists(out_file_path)) {
                Files.createDirectories(out_file_path.getParent());
                Files.createFile(out_file_path);
            }
            this.out_file_path = out_file_path;
        }
    }

    public void start(String props, String name, boolean use_fibers) throws Exception {
        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- MPerf -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
        sb.append("JGroups version: ").append(Version.description).append('\n');
        System.out.println(sb);

        thread_factory=new DefaultThreadFactory("invoker", false, true)
          .useFibers(use_fibers);
        if(use_fibers && Util.fibersAvailable())
            System.out.println("-- using fibers instead of threads");

        channel=new JChannel(props).setName(name).setReceiver(this)
          .connect("mperf");
        local_addr=channel.getAddress();

        // send a CONFIG_REQ to the current coordinator, so we can get the current config
        Address coord=channel.getView().getCoord();
        if(coord != null && !local_addr.equals(coord))
            send(coord, null, MPerfHeader.CONFIG_REQ, Message.Flag.RSVP);
    }

    public void setOutPath(Path out_file_path) {
        this.out_file_path=out_file_path;
    }


    protected void eventLoop() {
        final String INPUT=
          "[1] Start test [2] View [4] Threads (%d) [6] Time (%,ds) [7] Msg size (%s)\n" +
            "[8] Number of senders (%s) [o] Toggle OOB (%s) [l] Toggle measure local messages (%s)\n" +
            "[s] Display message sources (%s)\n" +
            "[x] Exit this [X] Exit all";

        while(looping) {
            try {
                int c=Util.keyPress(String.format(INPUT, num_threads, time, Util.printBytes(msg_size),
                                              num_senders <= 0? "all" : String.valueOf(num_senders),
                                              oob, log_local, display_msg_src));
                switch(c) {
                    case '1':
                        startTest();
                        break;
                    case '2':
                        System.out.println("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
                        break;
                    case '4':
                        configChange("num_threads");
                        break;
                    case '6':
                        configChange("time");
                        break;
                    case '7':
                        configChange("msg_size");
                        break;
                    case '8':
                        configChange("num_senders");
                        break;
                    case 'o':
                        ConfigChange change=new ConfigChange("oob", !oob);
                        send(null, change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
                        break;
                    case 'l':
                        ConfigChange log_local_change=new ConfigChange("log_local", !log_local);
                        send(null, log_local_change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
                        break;
                    case 's':
                        ConfigChange display_msg_src_change=new ConfigChange("display_msg_src", !display_msg_src);
                        send(null, display_msg_src_change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                    case 'X':
                        send(null, null, MPerfHeader.EXIT, Message.Flag.OOB);
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        stop();
    }

    protected void startTest() throws Exception {
        results.reset(new ArrayList<>(members));
        send(null, null, MPerfHeader.START_SENDING, Message.Flag.OOB);
        results.waitForAllResponses(time * 1_000L * 2);
        displayResults();
    }

    protected void displayResults() {
        printOutput("\nResults:\n");
        printOutput("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
        printOutput(printParameters());
        Map<Address,Result> tmp_results=results.getResults();
        for(Map.Entry<Address,Result> entry: tmp_results.entrySet()) {
            Result val=entry.getValue();
            if(val != null) {
                String resultString = entry.getKey() + ": " + computeStats(val.time, val.msgs, msg_size);
                if (display_msg_src) {
                    resultString += printPerSender(val.sources, val.received, msg_size, val.time);
                }
                System.out.println(resultString);
            }
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
            printOutput("\n===============================================================================");
            printOutput(" Average/node:    " + computeStats(total_time / num, total_msgs / num, msg_size));
            printOutput(" Average/cluster: " + computeStats(total_time/num, total_msgs, msg_size));
            printOutput("================================================================================\n\n");
        }
        else {
            printOutput("\n===============================================================================");
            printOutput(" Received no results");
            printOutput("================================================================================\n\n");
        }
    }

    protected String printParameters() {
        StringBuilder sb=new StringBuilder();
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("time=").append(time).append('\n');
        sb.append("msg_size=").append(msg_size).append('\n');
        sb.append("num_threads=").append(num_threads).append('\n');
        sb.append("num_senders=").append(num_senders).append('\n');
        sb.append("oob=").append(oob).append('\n');
        sb.append("log_local=").append(log_local).append('\n');
        sb.append("display_msg_src=").append(display_msg_src).append('\n');
        return sb.toString();
    }

    protected void printOutput(String s) {
        System.out.println(s);
        if (out_file_path != null && Files.isWritable(out_file_path)) {
            try {
                Files.writeString(out_file_path, s + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void configChange(String name) throws Exception {
        int tmp=Util.readIntFromStdin(name + ": ");
        ConfigChange change=new ConfigChange(name, tmp);
        send(null, change, MPerfHeader.CONFIG_CHANGE, Message.Flag.RSVP);
    }


    protected void sendNoException(Address target, Object payload, byte header, Message.Flag ... flags) {
        Message msg=new ObjectMessage(target, payload).setFlag(flags);
        if(header > 0)
            msg.putHeader(ID, new MPerfHeader(header));
        try {
            channel.send(msg);
        }
        catch(Exception e) {
            log.error("%s: failed sending message to %s: %s", local_addr, target, e);
        }
    }

    protected void send(Address target, Object payload, byte header, Message.Flag ... flags) throws Exception {
        Message msg=payload == null? new EmptyMessage(target) : new ObjectMessage(target, payload);
        if(flags != null)
            for(Message.Flag flag: flags)
                msg.setFlag(flag);
        if(header > 0)
            msg.putHeader(ID, new MPerfHeader(header));
        channel.send(msg);
    }



    public void stop() {
        looping=false;
        Util.close(channel);
    }


    public void receive(Message msg) {
        MPerfHeader hdr=msg.getHeader(ID);

        switch(hdr.type) {
            case MPerfHeader.DATA:
                if (log_local || !Objects.equals(msg.getSrc(), local_addr)) {
                    received_msgs_map.addMessage(msg.getSrc());
                }
                break;

            case MPerfHeader.START_SENDING:
                boolean isSender = true;
                if(num_senders > 0) {
                    int my_rank=Util.getRank(members, local_addr);
                    if(my_rank >= 0 && my_rank > num_senders)
                        isSender = false;
                }
                start_time=System.currentTimeMillis();
                Result r=sendMessages(isSender);
                System.out.println("-- done");
                sendNoException(msg.getSrc(), r, MPerfHeader.RESULT, Message.Flag.OOB);
                break;

            case MPerfHeader.RESULT:
                Result res=msg.getObject();
                results.add(msg.getSrc(), res);
                break;

            case MPerfHeader.CONFIG_CHANGE:
                handleConfigChange(msg.getObject());
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
                Util.close(channel);
                System.exit(0);
                break;

            default:
                System.err.println("header type " + hdr.type + " not recognized");
        }
    }

    public void receive(MessageBatch batch) {
        for(Message msg: batch) {
            byte type=((MPerfHeader)msg.getHeader(ID)).type;
            if(type == MPerfHeader.DATA) {
                if (log_local || !Objects.equals(msg.getSrc(), local_addr)) {
                    received_msgs_map.addMessage(msg.getSrc());
                }
            } else {
                receive(msg);
            }
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


    protected void handleConfigChange(ConfigChange config_change) {
        String attr_name=config_change.attr_name;
        try {
            Object attr_value=config_change.getValue();
            Field field=Util.getField(this.getClass(), attr_name);
            Util.setField(field,this,attr_value);
            System.out.println(config_change.attr_name + "=" + attr_value);
        }
        catch(Exception e) {
            System.err.println("failed applying config change for attr " + attr_name + ": " + e);
        }
    }

    protected void handleConfigRequest(Address sender) throws Exception {
        Configuration cfg=new Configuration();
        cfg.addChange("time",        time);
        cfg.addChange("msg_size",    msg_size);
        cfg.addChange("num_threads", num_threads);
        cfg.addChange("num_senders", num_senders);
        cfg.addChange("oob",         oob);
        cfg.addChange("log_local",   log_local);
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
        results.retainAll(mbrs);
    }

    
    protected Result sendMessages(boolean isSender) {
        final Thread[]       senders=new Thread[num_threads];
        final CountDownLatch latch=new CountDownLatch(1);
        final byte[]         payload=new byte[msg_size];
        final AtomicBoolean running = new AtomicBoolean(true);

        received_msgs_map.reset();

        if (isSender) {
            for (int i = 0; i < num_threads; i++) {
                Sender sender = new Sender(latch, running, payload);
                senders[i] = thread_factory.newThread(sender, "sender-" + i);
                senders[i].start();
            }
        }
        System.out.printf("-- running test for %d seconds with %d sender threads\n", time, isSender ? num_threads : 0);
        long interval=(long)((time * 1000.0) / 10.0);
        long start=System.currentTimeMillis();
        latch.countDown();
        for(int i=1; i <= 10; i++) {
            Util.sleep(interval);
            System.out.printf("%d: %s\n", i, received_msgs_map.printAverage(start, msg_size, display_msg_src));
        }
        running.set(false);
        if (isSender) {
            running.set(false);
            for (Thread s : senders) {
                try {
                    s.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        Map<Address, Long> counts = received_msgs_map.snapshot();
        Result result=new Result(System.currentTimeMillis() - start, received_msgs_map.totalCount(),
                counts.keySet().toArray(new Address[0]),
                Arrays.stream(counts.values().toArray(new Long[0]))
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .toArray());
        received_msgs_map.reset();
        return result;
    }

    protected String printAverage(long start_time) {
        long diff=System.currentTimeMillis()-start_time;
        double msgs_per_sec=received_msgs_map.totalCount() / (diff / 1000.0);
        double throughput=msgs_per_sec * msg_size;
        return String.format("%,.2f msgs/sec (%s/sec)", msgs_per_sec, Util.printBytes(throughput));
    }

    protected static String computeStats(long time, long msgs, int size) {
        double msgs_sec, throughput=0;
        msgs_sec=msgs / (time/1000.0);
        throughput=(msgs * size) / (time / 1000.0);
        return String.format("%,d msgs, %s received, time=%,d ms, msgs/sec=%,.2f, throughput=%s",
                             msgs, Util.printBytes(msgs * size), time, msgs_sec, Util.printBytes(throughput));
    }

    protected static String printPerSender(Address[] sender, long[] received, long msg_size, long diff) {
            if (sender == null || sender.length == 0)
                return "";

            StringJoiner joiner = new StringJoiner("  ");
            for (int i = 0; i < sender.length; i++) {
                double msg_rate = received[i] / (diff / 1000.0);
                joiner.add(String.format("%s: %,.2f msgs/sec (%s/sec)",
                        sender[i],
                        msg_rate,
                        Util.printBytes(msg_rate * msg_size)));
            }
            return String.format("[%s]", joiner);
    }
    
    protected class Sender implements Runnable {
        protected final CountDownLatch latch;
        protected final AtomicBoolean  running;
        protected final byte[]         payload;

        protected Sender(CountDownLatch l, AtomicBoolean running, byte[] payload) {
            latch=l;
            this.running=running;
            this.payload=payload;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(Exception e) {
                e.printStackTrace();
                return;
            }

            while(running.get()) {
                try {
                    Message msg=new BytesMessage(null, payload).putHeader(ID, new MPerfHeader(MPerfHeader.DATA));
                    if(oob)
                        msg.setFlag(Message.Flag.OOB);
                    channel.send(msg);
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

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeString(attr_name,out);
            Util.writeByteBuffer(attr_value, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
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

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(changes.size());
            for(ConfigChange change: changes)
                change.writeTo(out);
        }

        public void readFrom(DataInput in) throws IOException {
            int len=in.readInt();
            for(int i=0; i < len; i++) {
                ConfigChange change=new ConfigChange();
                change.readFrom(in);
                changes.add(change);
            }
        }
    }


    protected static class Result implements Streamable {
        protected long    time; // time in ms
        protected long    msgs; // number of messages received
        protected Address[] sources;
        protected long[] received;

        public Result() {
        }

        public Result(long time, long msgs, Address[] sources, long[] received) {
            this.time=time;
            this.msgs=msgs;
            this.sources = sources;
            this.received = received;
        }

        public int size() {
            int sources_size = sources == null ? 0 : sources.length * (sources[0].serializedSize() + Global.LONG_SIZE);
            return Bits.size(time) + Bits.size(msgs) + sources_size;
        }

        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(time, out);
            Bits.writeLongCompressed(msgs, out);
            int sources_length = sources == null ? 0 : sources.length;
            Bits.writeIntCompressed(sources_length, out);
            if (sources_length == 0) {
                return;
            }
            for (int i = 0; i < sources.length; i++) {
                Util.writeAddress(sources[i], out);
                Bits.writeLongCompressed(received[i], out);
            }
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            time=Bits.readLongCompressed(in);
            msgs=Bits.readLongCompressed(in);
            int sources_length = Bits.readIntCompressed(in);
            if (sources_length > 0) {
                sources = new Address[sources_length];
                received = new long[sources_length];
                for (int i = 0; i < sources_length; i++) {
                    sources[i] = Util.readAddress(in);
                    received[i] = Bits.readLongCompressed(in);
                }
            }


        }

        public String toString() {
            return msgs + " in " + time + " ms";
        }
    }

    protected static class MPerfHeader extends Header {
        protected static final byte DATA          =  1;
        protected static final byte START_SENDING =  2;
        protected static final byte RESULT        =  4;
        protected static final byte CONFIG_CHANGE =  6;
        protected static final byte CONFIG_REQ    =  7;
        protected static final byte CONFIG_RSP    =  8;
        protected static final byte EXIT          =  9;


        protected byte         type;

        public MPerfHeader() {}
        public MPerfHeader(byte type) {this.type=type;}
        public short getMagicId() {return 77;}
        public Supplier<? extends Header> create() {
            return MPerfHeader::new;
        }

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
        }

        public void readFrom(DataInput in) throws IOException {
            type=in.readByte();
        }

        public String toString() {
            return typeToString(type);
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case DATA:          return "DATA";
                case START_SENDING: return "START_SENDING";
                case RESULT:        return "RESULT";
                case CONFIG_CHANGE: return "CONFIG_CHANGE";
                case CONFIG_REQ:    return "CONFIG_REQ";
                case CONFIG_RSP:    return "CONFIG_RSP";
                case EXIT:          return "EXIT";
                default:            return "n/a";
            }
        }
    }

    protected static class MessageCounter {
        protected ConcurrentHashMap<Address,LongAdder> countMap;

        public MessageCounter() {
            this.countMap = new ConcurrentHashMap<>();
        }

        public void addMessage(Address source) {
            LongAdder adder = countMap.get(source);
            if (adder == null) {
                adder = new LongAdder();
                countMap.put(source, adder);
            }
            adder.increment();
            //countMap.computeIfAbsent(source, k -> new LongAdder()).increment();
        }

        public void addMessage(Address source, long count) {
            countMap.computeIfAbsent(source, k -> new LongAdder()).add(count);
        }

        public void reset() {
            countMap = new ConcurrentHashMap<>();
        }

        public long totalCount() {
            long total = 0;
            for (LongAdder adder : countMap.values()) {
                total += adder.sum();
            }
            return total;
        }

        public double totalRate(long diff) {
            return totalCount() / (diff / 1000.0);
        }

        public Map<Address, Long> snapshot() {
            return countMap.entrySet().stream()
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().sum()))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue,
                                              Math::addExact, TreeMap::new));
        }

        public String printAverage(long start_time, int msg_size, boolean display_msg_src) {
            Map<Address, Long> snapshot = snapshot();
            long diff=System.currentTimeMillis()-start_time;
            double msgs_per_sec=totalCount() / (diff / 1000.0);
            double throughput=msgs_per_sec * msg_size;

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%,.2f msgs/sec (%s/sec)", msgs_per_sec, Util.printBytes(throughput)));
            if (display_msg_src) {
                StringJoiner joiner = new StringJoiner("  ");
                for (Map.Entry<Address, Long> e : snapshot.entrySet()) {
                    double msg_rate = e.getValue() / (diff / 1000.0);
                    joiner.add(String.format("%s: %,.2f msgs/sec (%s/sec)",
                            e.getKey(),
                            msg_rate,
                            Util.printBytes(msg_rate * msg_size)));
                }
                sb.append(String.format("[%s]", joiner));
            }
            return sb.toString();
        }
    }


    public static void main(String[] args) throws IOException {
        String props=null, name=null;
        boolean run_event_loop=true, use_fibers=true;
        Path out_file_path=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-file".equals(args[i])) {
                out_file_path = Paths.get(args[++i]);
                continue;
            }
            if("-nohup".equals(args[i])) {
                run_event_loop=false;
                continue;
            }
            if("-use_fibers".equals(args[i])) {
                use_fibers=Boolean.parseBoolean(args[++i]);
                continue;
            }
            System.out.println("MPerf [-props <stack config>] [-name <logical name>] [-nohup] [-use_fibers true|false] [-file <file path>]");
            return;
        }

        final MPerf test=new MPerf(out_file_path);
        try {
            test.start(props, name, use_fibers);
            if(run_event_loop)
                test.eventLoop();
            else {
                for(;;)
                    Util.sleep(60000);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
