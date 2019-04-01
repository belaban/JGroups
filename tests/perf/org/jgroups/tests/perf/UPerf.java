package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.*;

import javax.management.MBeanServer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver. Mimicks the DIST mode in Infinispan
 *
 * @author Bela Ban
 */
public class UPerf extends ReceiverAdapter {
    private JChannel               channel;
    private Address                local_addr;
    private RpcDispatcher          disp;
    static final String            groupname="uperf";
    protected final List<Address>  members=new ArrayList<>();
    protected volatile View        view;
    protected volatile boolean     looping=true;
    protected Thread               event_loop_thread;
    protected final LongAdder      num_reads=new LongAdder();
    protected final LongAdder      num_writes=new LongAdder();



    // ============ configurable properties ==================
    @Property protected boolean sync=true, oob=true;
    @Property protected int     num_threads=100;
    @Property protected int     time=60; // in seconds
    @Property protected int     msg_size=1000;
    @Property protected int     anycast_count=2;
    @Property protected double  read_percentage=0.8; // 80% reads, 20% writes
    @Property protected boolean allow_local_gets=true;
    @Property protected boolean print_invokers;
    @Property protected boolean print_details;
    // ... add your own here, just don't forget to annotate them with @Property
    // =======================================================

    private static final Method[] METHODS=new Method[6];
    private static final short START                 =  0;
    private static final short GET                   =  1;
    private static final short PUT                   =  2;
    private static final short GET_CONFIG            =  3;
    private static final short SET                   =  4;
    private static final short QUIT_ALL              =  5;

    protected static final Field SYNC, OOB, NUM_THREADS, TIME, MSG_SIZE, ANYCAST_COUNT,
      READ_PERCENTAGE, ALLOW_LOCAL_GETS, PRINT_INVOKERS, PRINT_DETAILS;


    private final AtomicInteger COUNTER=new AtomicInteger(1);
    private byte[]              BUFFER=new byte[msg_size];
    protected static final String format=
      "[1] Start test [2] View [4] Threads (%d) [6] Time (%,ds) [7] Msg size (%s)" +
        "\n[s] Sync (%b) [o] OOB (%b)" +
        "\n[a] Anycast count (%d) [r] Read percentage (%.2f) " +
        "\n[l] local gets (%b) [d] print details (%b)  [i] print invokers (%b)" +
        "\n[v] Version [x] Exit [X] Exit all\n";


    static {
        try {
            METHODS[START]                 = UPerf.class.getMethod("startTest");
            METHODS[GET]                   = UPerf.class.getMethod("get", long.class);
            METHODS[PUT]                   = UPerf.class.getMethod("put", long.class, byte[].class);
            METHODS[GET_CONFIG]            = UPerf.class.getMethod("getConfig");
            METHODS[SET]                   = UPerf.class.getMethod("set", String.class, Object.class);
            METHODS[QUIT_ALL]              = UPerf.class.getMethod("quitAll");
            ClassConfigurator.add((short)11000, Results.class);
            SYNC=Util.getField(UPerf.class, "sync", true);
            OOB=Util.getField(UPerf.class, "oob", true);
            NUM_THREADS=Util.getField(UPerf.class, "num_threads", true);
            TIME=Util.getField(UPerf.class, "time", true);
            MSG_SIZE=Util.getField(UPerf.class, "msg_size", true);
            ANYCAST_COUNT=Util.getField(UPerf.class, "anycast_count", true);
            READ_PERCENTAGE=Util.getField(UPerf.class, "read_percentage", true);
            ALLOW_LOCAL_GETS=Util.getField(UPerf.class, "allow_local_gets", true);
            PRINT_INVOKERS=Util.getField(UPerf.class, "print_invokers", true);
            PRINT_DETAILS=Util.getField(UPerf.class, "print_details", true);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, String name, AddressGenerator generator, int bind_port) throws Throwable {
        channel=new JChannel(props).addAddressGenerator(generator).setName(name);
        if(bind_port > 0) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setBindPort(bind_port);
        }

        disp=new RpcDispatcher(channel, this).setMembershipListener(this).setMethodLookup(id -> METHODS[id])
          .setMarshaller(new UPerfMarshaller());
        channel.connect(groupname);
        local_addr=channel.getAddress();

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }

        if(members.size() < 2)
            return;
        Address coord=members.get(0);
        Config config=disp.callRemoteMethod(coord, new MethodCall(GET_CONFIG), new RequestOptions(ResponseMode.GET_ALL, 5000));
        if(config != null) {
            applyConfig(config);
            System.out.println("Fetched config from " + coord + ": " + config + "\n");
        }
        else
            System.err.println("failed to fetch config from " + coord);
    }

    void stop() {
        Util.close(disp, channel);
    }

    protected void startEventThread() {
        event_loop_thread=new Thread(UPerf.this::eventLoop,"EventLoop");
        event_loop_thread.setDaemon(true);
        event_loop_thread.start();
    }

    protected void stopEventThread() {
        Thread tmp=event_loop_thread;
        looping=false;
        if(tmp != null)
            tmp.interrupt();
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        this.view=new_view;
        System.out.println("** view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
    }


    // =================================== callbacks ======================================

    public Results startTest() throws Throwable {
        BUFFER=new byte[msg_size];

        System.out.printf("running for %d seconds\n", time);
        final CountDownLatch latch=new CountDownLatch(1);
        num_reads.reset(); num_writes.reset();

        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++) {
            invokers[i]=new Invoker(members, latch);
            invokers[i].start(); // waits on latch
        }

        long start=System.currentTimeMillis();
        latch.countDown();
        long interval=(long)((time * 1000.0) / 10.0);
        for(int i=1; i <= 10; i++) {
            Util.sleep(interval);
            System.out.printf("%d: %s\n", i, printAverage(start));
        }

        for(Invoker invoker: invokers)
            invoker.cancel();
        for(Invoker invoker: invokers)
            invoker.join();
        long total_time=System.currentTimeMillis() - start;

        System.out.println("");
        AverageMinMax avg_gets=null, avg_puts=null;
        for(Invoker invoker: invokers) {
            if(print_invokers)
                System.out.printf("invoker %s: gets %s puts %s\n", invoker.getId(),
                                  print(invoker.avgGets(), print_details), print(invoker.avgPuts(), print_details));
            if(avg_gets == null)
                avg_gets=invoker.avgGets();
            else
                avg_gets.merge(invoker.avgGets());
            if(avg_puts == null)
                avg_puts=invoker.avgPuts();
            else
                avg_puts.merge(invoker.avgPuts());
        }
        if(print_invokers)
            System.out.printf("\navg over all invokers: gets %s puts %s\n",
                              print(avg_gets, print_details), print(avg_puts, print_details));

        System.out.printf("\ndone (in %s ms)\n", total_time);
        return new Results((int)num_reads.sum(), (int)num_writes.sum(), total_time, avg_gets, avg_puts);
    }

    public void quitAll() {
        System.out.println("-- received quitAll(): shutting down");
        stopEventThread();
    }

    protected String printAverage(long start_time) {
        long tmp_time=System.currentTimeMillis() - start_time;
        long reads=num_reads.sum(), writes=num_writes.sum();
        double reqs_sec=(reads+writes) / (tmp_time / 1000.0);
        return String.format("%,.0f reqs/sec (%,d reads %,d writes)", reqs_sec, reads, writes);
    }


    public void set(String field_name, Object value) {
        Field field=Util.getField(this.getClass(),field_name);
        if(field == null)
            System.err.println("Field " + field_name + " not found");
        else {
            Util.setField(field, this, value);
            System.out.println(field.getName() + "=" + value);
        }
    }

    public byte[] get(@SuppressWarnings("UnusedParameters") long key) {
        return BUFFER;
    }


    @SuppressWarnings("UnusedParameters")
    public void put(long key, byte[] val) {
    }

    public Config getConfig() {
        Config config=new Config();
        for(Field field: Util.getAllDeclaredFields(UPerf.class)) {
            if(field.isAnnotationPresent(Property.class)) {
                config.add(field.getName(), Util.getField(field, this));
            }
        }
        return config;
    }

    protected void applyConfig(Config config) {
        for(Map.Entry<String,Object> entry: config.values.entrySet()) {
            Field field=Util.getField(getClass(), entry.getKey());
            Util.setField(field, this, entry.getValue());
        }
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() {

        while(looping) {
            try {
                int c=Util.keyPress(String.format(format, num_threads, time, Util.printBytes(msg_size),
                                                  sync, oob, anycast_count, read_percentage,
                                                  allow_local_gets, print_details, print_invokers));
                switch(c) {
                    case '1':
                        startBenchmark();
                        break;
                    case '2':
                        printView();
                        break;
                    case '4':
                        changeFieldAcrossCluster(NUM_THREADS, Util.readIntFromStdin("Number of sender threads: "));
                        break;
                    case '6':
                        changeFieldAcrossCluster(TIME, Util.readIntFromStdin("Time (secs): "));
                        break;
                    case '7':
                        changeFieldAcrossCluster(MSG_SIZE, Util.readIntFromStdin("Message size: "));
                        break;
                    case 'a':
                        int tmp=getAnycastCount();
                        if(tmp >= 0)
                            changeFieldAcrossCluster(ANYCAST_COUNT, tmp);
                        break;
                    case 'o':
                        changeFieldAcrossCluster(OOB, !oob);
                        break;
                    case 's':
                        changeFieldAcrossCluster(SYNC, !sync);
                        break;
                    case 'r':
                        double percentage=getReadPercentage();
                        if(percentage >= 0)
                            changeFieldAcrossCluster(READ_PERCENTAGE, percentage);
                        break;
                    case 'd':
                        changeFieldAcrossCluster(PRINT_DETAILS, !print_details);
                        break;
                    case 'i':
                        changeFieldAcrossCluster(PRINT_INVOKERS, !print_invokers);
                        break;
                    case 'l':
                        changeFieldAcrossCluster(ALLOW_LOCAL_GETS, !allow_local_gets);
                        break;
                    case 'v':
                        System.out.printf("Version: %s\n", Version.printVersion());
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                    case 'X':
                        try {
                            RequestOptions options=new RequestOptions(ResponseMode.GET_NONE, 0)
                              .flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
                            disp.callRemoteMethods(null, new MethodCall(QUIT_ALL), options);
                            break;
                        }
                        catch(Throwable t) {
                            System.err.println("Calling quitAll() failed: " + t);
                        }
                        break;
                    case '\n':
                    case '\r':
                        break;
                    default:
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        stop();
    }


    /** Kicks off the benchmark on all cluster nodes */
    void startBenchmark() {
        RspList<Results> responses=null;
        try {
            RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 0);
            options.flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
            responses=disp.callRemoteMethods(null, new MethodCall(START), options);
        }
        catch(Throwable t) {
            System.err.println("starting the benchmark failed: " + t);
            return;
        }

        long total_reqs=0;
        long total_time=0;
        AverageMinMax avg_gets=null, avg_puts=null;

        System.out.println("\n======================= Results: ===========================");
        for(Map.Entry<Address,Rsp<Results>> entry: responses.entrySet()) {
            Address mbr=entry.getKey();
            Rsp<Results> rsp=entry.getValue();
            Results result=rsp.getValue();
            if(result != null) {
                total_reqs+=result.num_gets + result.num_puts;
                total_time+=result.time;
                if(avg_gets == null)
                    avg_gets=result.avg_gets;
                else
                    avg_gets.merge(result.avg_gets);
                if(avg_puts == null)
                    avg_puts=result.avg_puts;
                else
                    avg_puts.merge(result.avg_puts);
            }
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec=total_reqs / ( total_time/ 1000.0);
        double throughput=total_reqs_sec * BUFFER.length;
        System.out.println("\n");
        System.out.println(Util.bold(String.format("Throughput: %,.2f reqs/sec/node (%s/sec)\n" +
                                                   "Roundtrip:  gets %s, puts %s\n",
                                                   total_reqs_sec, Util.printBytes(throughput),
                                                   print(avg_gets, print_details), print(avg_puts, print_details))));
        System.out.println("\n\n");
    }
    


    static double getReadPercentage() throws Exception {
        double tmp=Util.readDoubleFromStdin("Read percentage: ");
        if(tmp < 0 || tmp > 1.0) {
            System.err.println("read percentage must be >= 0 or <= 1.0");
            return -1;
        }
        return tmp;
    }

    int getAnycastCount() throws Exception {
        int tmp=Util.readIntFromStdin("Anycast count: ");
        View tmp_view=channel.getView();
        if(tmp > tmp_view.size()) {
            System.err.println("anycast count must be smaller or equal to the view size (" + tmp_view + ")\n");
            return -1;
        }
        return tmp;
    }


    protected void changeFieldAcrossCluster(Field field, Object value) throws Exception {
        disp.callRemoteMethods(null, new MethodCall(SET, field.getName(), value), RequestOptions.SYNC());
    }


    protected void printView() {
        System.out.printf("\n-- local: %s, view: %s\n", local_addr, view);
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }

    protected static String print(AverageMinMax avg, boolean details) {
        return details? String.format("min/avg/max = %,.2f/%,.2f/%,.2f us",
                                      avg.min() / 1000.0, avg.average() / 1000.0, avg.max() / 1000.0) :
          String.format("avg = %,.2f us", avg.average() / 1000.0);
    }

    protected static List<String> getSites(JChannel channel) {
        RELAY2 relay=channel.getProtocolStack().findProtocol(RELAY2.class);
        return relay != null? relay.siteNames() : new ArrayList<>(0);
    }

    /** Picks the next member in the view */
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

    protected class UPerfMarshaller implements Marshaller {
        public int estimatedSize(Object arg) {
            if(arg == null)
                return 2;
            if(arg instanceof byte[])
                return msg_size + 24;
            if(arg instanceof Long)
                return 10;
            return 50;
        }

        @Override
        public void objectToStream(Object obj, DataOutput out) throws IOException {
            Util.objectToStream(obj, out);
        }

        @Override
        public Object objectFromStream(DataInput in) throws IOException, ClassNotFoundException {
            return Util.objectFromStream(in);
        }
    }


    private class Invoker extends Thread {
        private final List<Address>  dests=new ArrayList<>();
        private final CountDownLatch latch;
        private final AverageMinMax  avg_gets=new AverageMinMax(), avg_puts=new AverageMinMax(); // in ns
        private final List<Address>  targets=new ArrayList<>(anycast_count);
        private volatile boolean     running=true;


        public Invoker(Collection<Address> dests, CountDownLatch latch) {
            this.latch=latch;
            this.dests.addAll(dests);
            setName("Invoker-" + COUNTER.getAndIncrement());
        }

        
        public AverageMinMax avgGets() {return avg_gets;}
        public AverageMinMax avgPuts() {return avg_puts;}
        public void          cancel()  {running=false;}

        public void run() {
            Object[] put_args={0, BUFFER};
            Object[] get_args={0};
            MethodCall get_call=new MethodCall(GET, get_args);
            MethodCall put_call=new MethodCall(PUT, put_args);
            RequestOptions get_options=new RequestOptions(ResponseMode.GET_ALL, 40000, false, null);
            RequestOptions put_options=new RequestOptions(sync ? ResponseMode.GET_ALL : ResponseMode.GET_NONE, 40000, true, null);

            if(oob) {
                get_options.flags(Message.Flag.OOB);
                put_options.flags(Message.Flag.OOB);
            }

            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }

            while(running) {
                boolean get=Util.tossWeightedCoin(read_percentage);

                try {
                    if(get) { // sync GET
                        Address target=pickTarget();
                        long start=System.nanoTime();
                        if(allow_local_gets && Objects.equals(target, local_addr))
                            get(1);
                        else {
                            disp.callRemoteMethod(target, get_call, get_options);
                        }
                        long get_time=System.nanoTime()-start;
                        avg_gets.add(get_time);
                        num_reads.increment();
                    }
                    else {    // sync or async (based on value of 'sync') PUT
                        pickAnycastTargets(targets);
                        long start=System.nanoTime();
                        disp.callRemoteMethods(targets, put_call, put_options);
                        long put_time=System.nanoTime()-start;
                        avg_puts.add(put_time);
                        num_writes.increment();
                    }
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }

        private Address pickTarget() {
            return Util.pickRandomElement(dests);
        }

        private void pickAnycastTargets(List<Address> anycast_targets) {
            int index=dests.indexOf(local_addr);
            for(int i=index + 1; i < index + 1 + anycast_count; i++) {
                int new_index=i % dests.size();
                Address tmp=dests.get(new_index);
                if(!anycast_targets.contains(tmp))
                    anycast_targets.add(tmp);
            }
        }
    }


    public static class Results implements Streamable {
        protected long          num_gets;
        protected long          num_puts;
        protected long          time;     // in ms
        protected AverageMinMax avg_gets; // RTT in ns
        protected AverageMinMax avg_puts; // RTT in ns

        public Results() {
            
        }

        public Results(int num_gets, int num_puts, long time, AverageMinMax avg_gets, AverageMinMax avg_puts) {
            this.num_gets=num_gets;
            this.num_puts=num_puts;
            this.time=time;
            this.avg_gets=avg_gets;
            this.avg_puts=avg_puts;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLong(num_gets, out);
            Bits.writeLong(num_puts, out);
            Bits.writeLong(time, out);
            Util.writeStreamable(avg_gets, out);
            Util.writeStreamable(avg_puts, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_gets=Bits.readLong(in);
            num_puts=Bits.readLong(in);
            time=Bits.readLong(in);
            avg_gets=Util.readStreamable(AverageMinMax::new, in);
            avg_puts=Util.readStreamable(AverageMinMax::new, in);
        }

        public String toString() {
            long total_reqs=num_gets + num_puts;
            double total_reqs_per_sec=total_reqs / (time / 1000.0);
            return String.format("%,.2f reqs/sec (%,d gets, %,d puts, get RTT %,.2f us, put RTT %,.2f us)",
                                 total_reqs_per_sec, num_gets, num_puts, avg_gets.average() / 1000.0,
                                 avg_puts.getAverage()/1000.0);
        }
    }


    public static class Config implements Streamable {
        protected Map<String,Object> values=new HashMap<>();

        public Config() {
        }

        public Config add(String key, Object value) {
            values.put(key, value);
            return this;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(values.size());
            for(Map.Entry<String,Object> entry: values.entrySet()) {
                Bits.writeString(entry.getKey(),out);
                Util.objectToStream(entry.getValue(), out);
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size=in.readInt();
            for(int i=0; i < size; i++) {
                String key=Bits.readString(in);
                Object value=Util.objectFromStream(in);
                if(key == null)
                    continue;
                values.put(key, value);
            }
        }

        public String toString() {
            return values.toString();
        }
    }




    public static void main(String[] args) {
        String  props=null;
        String  name=null;
        boolean run_event_loop=true;
        AddressGenerator addr_generator=null;
        int port=0;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-nohup".equals(args[i])) {
                run_event_loop=false;
                continue;
            }
            if("-uuid".equals(args[i])) {
                addr_generator=new OneTimeAddressGenerator(Long.valueOf(args[++i]));
                continue;
            }
            if("-port".equals(args[i])) {
                port=Integer.valueOf(args[++i]);
                continue;
            }
            help();
            return;
        }

        UPerf test=null;
        try {
            test=new UPerf();
            test.init(props, name, addr_generator, port);
            if(run_event_loop)
                test.startEventThread();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UPerf [-props <props>] [-name name] [-nohup] [-uuid <UUID>] [-port <bind port>]");
    }


}