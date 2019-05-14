package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.NonReflectiveProbeHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver. Mimicks the DIST mode in Infinispan
 *
 * @author Bela Ban
 */
public class ProgrammaticUPerf extends ReceiverAdapter implements MethodInvoker {
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

    private static final short START                 =  0;
    private static final short GET                   =  1;
    private static final short PUT                   =  2;
    private static final short GET_CONFIG            =  3;
    private static final short SET_SYNC              =  4;
    private static final short SET_OOB               =  5;
    private static final short SET_NUM_THREADS       =  6;
    private static final short SET_TIME              =  7;
    private static final short SET_MSG_SIZE          =  8;
    private static final short SET_ANYCAST_COUNT     =  9;
    private static final short SET_READ_PERCENTAGE   = 10;
    private static final short ALLOW_LOCAL_GETS      = 11;
    private static final short PRINT_INVOKERS        = 12;
    private static final short PRINT_DETAILS         = 13;
    private static final short QUIT_ALL              = 14;


    private final AtomicInteger COUNTER=new AtomicInteger(1);
    private byte[]              BUFFER=new byte[msg_size];
    protected static final String format=
      "[1] Start test [2] View [4] Threads (%d) [6] Time (%,ds) [7] Msg size (%s)" +
        "\n[s] Sync (%b) [o] OOB (%b)" +
        "\n[a] Anycast count (%d) [r] Read percentage (%.2f) " +
        "\n[l] local gets (%b) [d] print details (%b)  [i] print invokers (%b)" +
        "\n[v] Version [x] Exit [X] Exit all\n";



    public boolean getSync()                   {return sync;}
    public void    setSync(boolean s)          {this.sync=s;}
    public boolean getOOB()                    {return oob;}
    public void    setOOB(boolean oob)         {this.oob=oob;}
    public int     getNumThreads()             {return num_threads;}
    public void    setNumThreads(int t)        {this.num_threads=t;}
    public int     getTime()                   {return time;}
    public void    setTime(int t)              {this.time=t;}
    public int     getMsgSize()                {return msg_size;}
    public void    setMsgSize(int t)           {this.msg_size=t;}
    public int     getAnycastCount()           {return anycast_count;}
    public void    setAnycastCount(int t)      {this.anycast_count=t;}
    public double  getReadPercentage()         {return read_percentage;}
    public void    setReadPercentage(double r) {this.read_percentage=r;}
    public boolean allowLocalGets()            {return allow_local_gets;}
    public void    allowLocalGets(boolean a)   {this.allow_local_gets=a;}
    public boolean printInvokers()             {return print_invokers;}
    public void    printInvokers(boolean p)    {this.print_invokers=p;}
    public boolean printDetails()              {return print_details;}
    public void    printDetails(boolean p)     {this.print_details=p;}




    public void init(String name, AddressGenerator generator, String bind_addr, int bind_port) throws Throwable {

        InetAddress bind_address=bind_addr != null? Util.getAddress(bind_addr, Util.getIpStackType()) : Util.getLoopback();

        // bind_address=InetAddress.getByName("::1"); // todo:" remove!!!

        Protocol[] prot_stack={
          new TCP().setBindAddress(bind_address).setBindPort(7800)
            .setDiagnosticsEnabled(true)
            .diagEnableUdp(false) // todo: enable when MulticastSocket works
            .diagEnableTcp(true),
          new TCPPING().initialHosts(Collections.singletonList(new InetSocketAddress(bind_address, 7800))),
          new MERGE3(),
          new FD_SOCK(),
          new FD_ALL(),
          new VERIFY_SUSPECT(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(1000),
          new UFC(),
          new MFC(),
          new FRAG2()};
        channel=new JChannel(prot_stack).addAddressGenerator(generator).setName(name);
        TP transport=channel.getProtocolStack().getTransport();
        if(bind_port > 0) {
            transport=channel.getProtocolStack().getTransport();
            transport.setBindPort(bind_port);
        }
        // todo: remove default ProbeHandler for "jmx" and "op"
        NonReflectiveProbeHandler h=new NonReflectiveProbeHandler(channel);
        transport.registerProbeHandler(h);
        h.initialize(channel.getProtocolStack().getProtocols());
        // System.out.printf("contents:\n%s\n", h.dump());
        disp=new RpcDispatcher(channel, this).setMembershipListener(this)
          .setMethodInvoker(this).setMarshaller(new UPerfMarshaller());
        channel.connect(groupname);
        local_addr=channel.getAddress();
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
        event_loop_thread=new Thread(ProgrammaticUPerf.this::eventLoop, "EventLoop");
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

    public Object invoke(Object target, short method_id, Object[] args) throws Exception {
        ProgrammaticUPerf uperf=(ProgrammaticUPerf)target;
        Boolean bool_val;
        switch(method_id) {
            case START:
                return uperf.startTest();
            case GET:
                Integer key=(Integer)args[0];
                return uperf.get(key);
            case PUT:
                key=(Integer)args[0];
                byte[] val=(byte[])args[1];
                uperf.put(key, val);
                return null;
            case GET_CONFIG:
                return uperf.getConfig();
            case SET_SYNC:
                uperf.setSync((Boolean)args[0]);
                return null;
            case SET_OOB:
                bool_val=(Boolean)args[0];
                uperf.setOOB(bool_val);
                return null;
            case SET_NUM_THREADS:
                uperf.setNumThreads((Integer)args[0]);
                return null;
            case SET_TIME:
                uperf.setTime((Integer)args[0]);
                return null;
            case SET_MSG_SIZE:
                uperf.setMsgSize((Integer)args[0]);
                return null;
            case SET_ANYCAST_COUNT:
                uperf.setAnycastCount((Integer)args[0]);
                return null;
            case SET_READ_PERCENTAGE:
                uperf.setReadPercentage((Double)args[0]);
                return null;
            case ALLOW_LOCAL_GETS:
                uperf.allowLocalGets((Boolean)args[0]);
                return null;
            case PRINT_INVOKERS:
                uperf.printInvokers((Boolean)args[0]);
                return null;
            case PRINT_DETAILS:
                uperf.printDetails((Boolean)args[0]);
                return null;
            case QUIT_ALL:
                uperf.quitAll();
                return null;
            default:
                throw new IllegalArgumentException("method with id=" + method_id + " not found");
        }
    }

    // =================================== callbacks ======================================

    public Results startTest() throws Exception {
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

        System.out.println();
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


    public byte[] get(@SuppressWarnings("UnusedParameters")int key) {
        return BUFFER;
    }


    @SuppressWarnings("UnusedParameters")
    public void put(int key, byte[] val) {
    }

    public Config getConfig() {
        Config c=new Config();
        c.add("sync", sync).add("oob", oob).add("num_threads", num_threads).add("time", time).add("msg_size", msg_size)
          .add("anycast_count", anycast_count).add("read_percentage", read_percentage)
          .add("allow_local_gets", allow_local_gets).add("print_invokers", print_invokers).add("print_details", print_details);
        return c;
    }

    protected void applyConfig(Config config) {
        for(Map.Entry<String,Object> e: config.values.entrySet()) {
            String name=e.getKey();
            Object value=e.getValue();
            switch(name) {
                case "sync":
                    setSync((Boolean)value);
                    break;
                case "oob":
                    setOOB((Boolean)value);
                    break;
                case "num_threads":
                    setNumThreads((Integer)value);
                    break;
                case "time":
                    setTime((Integer)value);
                    break;
                case "msg_size":
                    setMsgSize((Integer)value);
                    break;
                case "anycast_count":
                    setAnycastCount((Integer)value);
                    break;
                case "read_percentage":
                    setReadPercentage((Double)value);
                    break;
                case "allow_local_gets":
                    allowLocalGets((Boolean)value);
                    break;
                case "print_invokers":
                    printInvokers((Boolean)value);
                    break;
                case "print_details":
                    printDetails((Boolean)value);
                    break;
                default:
                    throw new IllegalArgumentException("field with name " + name + " not known");
            }
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
                        invoke(SET_NUM_THREADS, Util.readIntFromStdin("Number of sender threads: "));
                        break;
                    case '6':
                        invoke(SET_TIME, Util.readIntFromStdin("Time (secs): "));
                        break;
                    case '7':
                        invoke(SET_MSG_SIZE, Util.readIntFromStdin("Message size: "));
                        break;
                    case 'a':
                        int tmp=parseAnycastCount();
                        if(tmp >= 0)
                            invoke(SET_ANYCAST_COUNT, tmp);
                        break;
                    case 'o':
                        invoke(SET_OOB, !oob);
                        break;
                    case 's':
                        invoke(SET_SYNC, !sync);
                        break;
                    case 'r':
                        double percentage=parseReadPercentage();
                        if(percentage >= 0)
                            invoke(SET_READ_PERCENTAGE, percentage);
                        break;
                    case 'd':
                        invoke(PRINT_DETAILS, !print_details);
                        break;
                    case 'i':
                        invoke(PRINT_INVOKERS, !print_invokers);
                        break;
                    case 'l':
                        invoke(ALLOW_LOCAL_GETS, !allow_local_gets);
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

    void invoke(short method_id, Object... args) throws Exception {
        MethodCall call=new MethodCall(method_id, args);
        disp.callRemoteMethods(null, call, RequestOptions.SYNC());
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
    


    static double parseReadPercentage() throws Exception {
        double tmp=Util.readDoubleFromStdin("Read percentage: ");
        if(tmp < 0 || tmp > 1.0) {
            System.err.println("read percentage must be >= 0 or <= 1.0");
            return -1;
        }
        return tmp;
    }

    int parseAnycastCount() throws Exception {
        int tmp=Util.readIntFromStdin("Anycast count: ");
        View tmp_view=channel.getView();
        if(tmp > tmp_view.size()) {
            System.err.println("anycast count must be smaller or equal to the view size (" + tmp_view + ")\n");
            return -1;
        }
        return tmp;
    }


    protected void printView() {
        System.out.printf("\n-- local: %s, view: %s\n", local_addr, view);
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception ignored) {
        }
    }

    protected static String print(AverageMinMax avg, boolean details) {
        return details? String.format("min/avg/max = %,.2f/%,.2f/%,.2f us",
                                      avg.min() / 1000.0, avg.average() / 1000.0, avg.max() / 1000.0) :
          String.format("avg = %,.2f us", avg.average() / 1000.0);
    }


    protected class UPerfMarshaller implements Marshaller {
        protected static final byte NORMAL=0, EXCEPTION=1, CONFIG=2,RESULTS=3;

        public int estimatedSize(Object arg) {
            if(arg == null)
                return 2;
            if(arg instanceof byte[])
                return msg_size + 24;
            if(arg instanceof Long)
                return 10;
            return 50;
        }

        // Unless and until serialization is supported on GraalVM, we're sending only the exception message across the
        // wire, but not the entire exception
        public void objectToStream(Object obj, DataOutput out) throws IOException {
            if(obj instanceof Throwable) {
                Throwable t=(Throwable)obj;
                out.writeByte(EXCEPTION);
                out.writeUTF(t.getMessage());
                return;
            }
            if(obj instanceof Config) {
                out.writeByte(CONFIG);
                ((Config)obj).writeTo(out);
                return;
            }
            if(obj instanceof Results) {
                out.writeByte(RESULTS);
                ((Results)obj).writeTo(out);
                return;
            }
            out.writeByte(NORMAL);
            Util.objectToStream(obj, out);
        }

        public Object objectFromStream(DataInput in) throws IOException, ClassNotFoundException {
            byte type=in.readByte();
            switch(type) {
                case NORMAL:
                    return Util.objectFromStream(in);
                case EXCEPTION:  // read exception
                    String message=in.readUTF();
                    return new RuntimeException(message);
                case CONFIG:
                    Config cfg=new Config();
                    cfg.readFrom(in);
                    return cfg;
                case RESULTS:
                    Results res=new Results();
                    res.readFrom(in);
                    return res;
                default:
                    throw new IllegalArgumentException("type " + type + " not known");
            }
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

        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLong(num_gets, out);
            Bits.writeLong(num_puts, out);
            Bits.writeLong(time, out);
            Util.writeStreamable(avg_gets, out);
            Util.writeStreamable(avg_puts, out);
        }

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

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(values.size());
            for(Map.Entry<String,Object> entry: values.entrySet()) {
                Bits.writeString(entry.getKey(),out);
                Util.objectToStream(entry.getValue(), out);
            }
        }

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
        String  name=null, bind_addr=null;
        boolean run_event_loop=true, jmx=false;
        AddressGenerator addr_generator=null;
        int port=0;

        for(int i=0; i < args.length; i++) {
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
            if("-bind_addr".equals(args[i])) {
                bind_addr=args[++i];
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        ProgrammaticUPerf test=null;
        try {
            test=new ProgrammaticUPerf();
            test.init(name, addr_generator, bind_addr, port);
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
        System.out.printf("%s [-name name] [-nohup] [-uuid <UUID>] [-port <bind port>] " +
                             "[-bind_addr bind-address]\n", ProgrammaticUPerf.class.getSimpleName());
    }


}