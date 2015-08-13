package org.jgroups.protocols.jzookeeper;


import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import javax.management.MBeanServer;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver. Mimicks the DIST mode in Infinispan
 *
 * @author Bela Ban
 */
public class TestAll extends ReceiverAdapter {
    private JChannel               channel;
    private Address                local_addr;
    private RpcDispatcher          disp;
    static final String            groupname="ZABCluster";
	private String propsFile = "conf/ZAB.xml";
	private static String ProtocotName = "ZAB";
    protected final List<Address>  members=new ArrayList<Address>();
    protected final List<Address>  site_masters=new ArrayList<Address>();
    private List<String> boxMembers  = new ArrayList<String>();
	private List<Address> zabBox = new ArrayList<Address>();
    private String outputDir;
	private View view;


    // ============ configurable properties ==================
    private boolean sync=true, oob=false, anycastRequests=true;
    private int num_threads=25, numOfClients=10, timeout=5000, load = 1;
    private int num_msgs=10000, msg_size=1000, num_msgsPerThreads = 10000;
    private int numsOfWarmUpPerThread = 0;
    private int anycast_count=1;
    private boolean use_anycast_addrs = true;
    private boolean random_destinations = false;
    private boolean include_local_address = true; // Include local address in anycast count
    private double read_percentage=0; // 80% reads, 20% writes
    // =======================================================

    private static final Method[] METHODS=new Method[15];

    private static final short START                 =  0;
    private static final short SET_OOB               =  1;
    private static final short SET_SYNC              =  2;
    private static final short SET_NUM_MSGS          =  3;
    private static final short SET_NUM_THREADS       =  4;
    private static final short SET_MSG_SIZE          =  5;
    private static final short SET_ANYCAST_COUNT     =  6;
    private static final short SET_USE_ANYCAST_ADDRS =  7;
    private static final short SET_READ_PERCENTAGE   =  8;
    private static final short GET                   =  9;
    private static final short PUT                   = 10;
    private static final short GET_CONFIG            = 11;

    private final AtomicInteger COUNTER=new AtomicInteger(1);
    private byte[] GET_RSP=new byte[msg_size];

    static NumberFormat f;


    static {
        try {
            METHODS[START]                 = TestAll.class.getMethod("startTest");
            METHODS[SET_OOB]               = TestAll.class.getMethod("setOOB", boolean.class);
            METHODS[SET_SYNC]              = TestAll.class.getMethod("setSync", boolean.class);
            METHODS[SET_NUM_MSGS]          = TestAll.class.getMethod("setNumMessages", int.class);
            METHODS[SET_NUM_THREADS]       = TestAll.class.getMethod("setNumThreads", int.class);
            METHODS[SET_MSG_SIZE]          = TestAll.class.getMethod("setMessageSize", int.class);
            METHODS[SET_ANYCAST_COUNT]     = TestAll.class.getMethod("setAnycastCount", int.class);
            METHODS[SET_USE_ANYCAST_ADDRS] = TestAll.class.getMethod("setUseAnycastAddrs", boolean.class);
            METHODS[SET_READ_PERCENTAGE]   = TestAll.class.getMethod("setReadPercentage", double.class);
            METHODS[GET]                   = TestAll.class.getMethod("get", long.class);
            METHODS[PUT]                   = TestAll.class.getMethod("put", long.class, byte[].class);
            METHODS[GET_CONFIG]            = TestAll.class.getMethod("getConfig");

            ClassConfigurator.add((short)11000, Results.class);
            f=NumberFormat.getNumberInstance();
            f.setGroupingUsed(false);
            f.setMinimumFractionDigits(2);
            f.setMaximumFractionDigits(2);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(List<String> zabMembers, String protocolName, String props,
			 int totalNum_msgs, int totalPerThreads, int num_threads,
			 int msg_size, String outputDir, int numOfClients, int load, int numsOfWarmUpPerThread) throws Throwable {
    	this.ProtocotName = protocolName;
    	this.propsFile = props;
    	this.num_msgs = totalNum_msgs;
		this.num_msgsPerThreads = totalPerThreads;
		this.num_threads = num_threads;
		this.msg_size = msg_size;
		this.outputDir = outputDir;
		this.numOfClients = numOfClients;
		this.load = load;
		this.numsOfWarmUpPerThread = numsOfWarmUpPerThread;

        channel=new JChannel(propsFile);
        System.out.println("this.ProtocotName := " + this.ProtocotName+ " propsFile :="+propsFile+
        		"this.num_threads "+this.num_threads);

        disp=new RpcDispatcher(channel, null, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return METHODS[id];
            }
        });
        disp.setRequestMarshaller(new CustomMarshaller());
		channel.connect("ZABCLRPC");
        local_addr=channel.getAddress();
        this.boxMembers = zabMembers;

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }

//        if(members.size() < 2)
//            return;

        while (members.size() < 3)
            Util.sleep(1);

        Address coord = pickCoordinator();
        System.out.println("Coordinator := " + coord);
//        ConfigOptions config=(ConfigOptions)disp.callRemoteMethod(coord, new MethodCall(GET_CONFIG), new RequestOptions(ResponseMode.GET_ALL, timeout));
//        if(config != null) {
//            this.oob=config.oob;
//            this.sync=config.sync;
//            this.num_threads=config.num_threads;
//            this.num_msgs=config.num_msgs;
//            this.msg_size=config.msg_size;
//            this.anycast_count=config.anycast_count;
//            this.use_anycast_addrs=config.use_anycast_addrs;
//            this.read_percentage=config.read_percentage;
//            System.out.println("Fetched config from " + coord + ": " + config);
//        }
//        else
//            System.err.println("failed to fetch config from " + coord);
    }

    void stop() {
        if(disp != null)
            disp.stop();
        Util.close(channel);
    }

    private Address pickCoordinator() {
        if (boxMembers == null) {
            System.out.println("Box Members is null");
            return members.get(0);
        }
        for (Address address : members) {
            for (String boxName : boxMembers) {
                if (!address.toString().contains(boxName))
                    return address;
            }
        }
        return null;
    }

    public void viewAccepted(View new_view) {
        List<Address> addresses = new ArrayList<Address>(new_view.getMembers());
        synchronized (members) {
            members.clear();
            members.addAll(addresses);
            removeBoxMembers(members);
        }
        System.out.println("** Members: " + members);
        
        
        System.out.println("** view: " + new_view);
		view = new_view;
		List<Address> mbrs = new_view.getMembers();
		if (mbrs.size() == 3) {
			zabBox.addAll(addresses);
		}

		if (mbrs.size() > 3 && zabBox.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabBox.add(addresses.get(i));
			}
		}
		local_addr=channel.getAddress();
		
		
        //addSiteMastersToMembers();          		   		
    	
    }
    
    

    protected void addSiteMastersToMembers() {
        if(!site_masters.isEmpty()) {
            for(Address sm: site_masters)
                if(!members.contains(sm))
                    members.add(sm);
        }
    }

    // =================================== callbacks ======================================

    private void removeBoxMembers(Collection<Address> addresses) {
        if (boxMembers == null)
            return;

        Iterator<Address> i = addresses.iterator();
        while (i.hasNext()) {
            Address address = i.next();
            for (String boxName : boxMembers) {
                if (address.toString().contains(boxName)) {
                    System.out.println("Remove | " + address);
                    i.remove();
                }
            }
        }
    }

    public Results startTest() throws Throwable {
    	 System.out.println("Start hereeeeeeeeeeeeeeeeeeeeeeee");
        addSiteMastersToMembers();

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) +
                ", sync=" + sync + ", oob=" + oob + ", use_anycast_addrs=" + use_anycast_addrs);
        int total_gets=0, total_puts=0;
        final AtomicInteger num_msgs_sent=new AtomicInteger(0);

        List<Address> nonBoxMembers = new ArrayList<Address>(members);
        removeBoxMembers(nonBoxMembers);

        Random random = new Random();
        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++){
            invokers[i]=new Invoker(nonBoxMembers, num_msgs, num_msgs_sent, random);
            System.out.println("Create Invoker --------------->>>> " + i);

        }

        long start=System.currentTimeMillis();
        for(Invoker invoker: invokers)
            invoker.start();

        for(Invoker invoker: invokers) {
            invoker.join();
            total_gets+=invoker.numGets();
            total_puts+=invoker.numPuts();
        }

        long total_time=System.currentTimeMillis() - start;
        System.out.println("done (in " + total_time + " ms)");
        return new Results(total_gets, total_puts, total_time);
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

    public void setAnycastCount(int num) {
        anycast_count=num;
        System.out.println("anycast_count = " + anycast_count);
    }

    public void setUseAnycastAddrs(boolean flag) {
        use_anycast_addrs=flag;
        System.out.println("use_anycast_addrs = " + use_anycast_addrs);
    }

    public void setReadPercentage(double val) {
        this.read_percentage=val;
        System.out.println("read_percentage = " + read_percentage);
    }

    public byte[] get(long key) {
        return GET_RSP;
    }


    public void put(long key, byte[] val) {
    	System.out.println("Inside -----------> PUT method**************");
    }

    public ConfigOptions getConfig() {
    	System.out.println("Inside -----------> getConfig");
        return new ConfigOptions(oob, sync, num_threads, num_msgs, msg_size, anycast_count, use_anycast_addrs, read_percentage);
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() throws Throwable {
        int c;

        addSiteMastersToMembers();

        Collection<Address> dest;
        if (anycastRequests)
            dest = members;
        else
            dest = null;

        while(true) {
            c=Util.keyPress("[1] Send msgs [2] Print view [3] Print conns " +
                    "[4] Trash conn [5] Trash all conns" +
                    "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    " [9] Set anycast count (" + anycast_count + ")" +
                    "\n[o] Toggle OOB (" + oob + ") [s] Toggle sync (" + sync +
                    ") [r] Set read percentage (" + f.format(read_percentage) + ") " +
                    "[a] Toggle use_anycast_addrs (" + use_anycast_addrs + ")" +
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
                    setSenderThreads(dest);
                    break;
                case '7':
                    setNumMessages(dest);
                    break;
                case '8':
                    setMessageSize(dest);
                    break;
                case '9':
                    setAnycastCount(dest);
                    break;
                case 'a':
                    boolean new_value=!use_anycast_addrs;
                    disp.callRemoteMethods(dest, new MethodCall(SET_USE_ANYCAST_ADDRS, new_value), RequestOptions.SYNC());
                    break;
                case 'o':
                    new_value=!oob;
                    disp.callRemoteMethods(dest, new MethodCall(SET_OOB, new_value), RequestOptions.SYNC());
                    break;
                case 's':
                    boolean new_val=!sync;
                    disp.callRemoteMethods(dest, new MethodCall(SET_SYNC, new_val), RequestOptions.SYNC());
                    break;
                case 'r':
                    setReadPercentage(dest);
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
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            System.out.println("connections:\n" + ((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println("connections:\n" + ((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
    }


    /** Kicks off the benchmark on all cluster nodes */
    void startBenchmark() throws Throwable {
        synchronized (members) {
            removeBoxMembers(members);
        }
        System.out.println("Members at start | " + members);

        Collection<Address> dest;
        if (anycastRequests)
            dest = members;
        else
            dest = null;

        RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 0, anycastRequests);
        options.setFlags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.NO_FC);
        RspList<Object> responses=disp.callRemoteMethods(dest, new MethodCall(START), options);
        System.out.println("after sending rpc");

        long total_reqs=0;
        long total_time=0;

        System.out.println("\n======================= Results: ===========================");
        for(Map.Entry<Address,Rsp<Object>> entry: responses.entrySet()) {
            Address mbr=entry.getKey();
            Rsp rsp=entry.getValue();
            Results result=(Results)rsp.getValue();
            total_reqs+=result.num_gets + result.num_puts;
            total_time+=result.time;
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec=total_reqs / ( total_time/ 1000.0);
        double throughput=total_reqs_sec * msg_size;
        double ms_per_req=total_time / (double)total_reqs;
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        System.out.println("\n");
        System.out.println(Util.bold("Average of " + f.format(total_reqs_sec) + " requests / sec (" +
                Util.printBytes(throughput) + " / sec), " +
                f.format(ms_per_req) + " ms /request (prot=" + prot.getName() + ")"));
        System.out.println("\n\n");
    }


    void setSenderThreads(Collection<Address> dest) throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        disp.callRemoteMethods(dest, new MethodCall(SET_NUM_THREADS, threads), RequestOptions.SYNC());
    }

    void setNumMessages(Collection<Address> dest) throws Exception {
        int tmp=Util.readIntFromStdin("Number of RPCs: ");
        disp.callRemoteMethods(dest, new MethodCall(SET_NUM_MSGS, tmp), RequestOptions.SYNC());
    }

    void setMessageSize(Collection<Address> dest) throws Exception {
        int tmp=Util.readIntFromStdin("Message size: ");
        disp.callRemoteMethods(dest, new MethodCall(SET_MSG_SIZE, tmp), RequestOptions.SYNC());
    }

    void setReadPercentage(Collection<Address> dest) throws Exception {
        double tmp=Util.readDoubleFromStdin("Read percentage: ");
        if(tmp < 0 || tmp > 1.0) {
            System.err.println("read percentage must be >= 0 or <= 1.0");
            return;
        }
        disp.callRemoteMethods(dest, new MethodCall(SET_READ_PERCENTAGE, tmp), RequestOptions.SYNC());
    }

    void setAnycastCount(Collection<Address> dest) throws Exception {
        int tmp=Util.readIntFromStdin("Anycast count: ");

        if(tmp > members.size()) {
            System.err.println("anycast count must be smaller or equal to the number of client nodes (" + members.size() + ")\n");
            return;
        }
        disp.callRemoteMethods(dest, new MethodCall(SET_ANYCAST_COUNT, tmp), RequestOptions.SYNC());
    }



    void printView() {
        System.out.println("\n-- view: " + members + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }

    protected static List<String> getSites(JChannel channel) {
        RELAY2 relay=(RELAY2)channel.getProtocolStack().findProtocol(RELAY2.class);
        return relay != null? relay.siteNames() : new ArrayList<String>(0);
    }

    /** Picks the next member in the view */
    private Address getReceiver() {
        try {
            List<Address> mbrs = members;
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
        private final List<Address>  dests=new ArrayList<Address>();
        private final int            num_msgs_to_send;
        private final AtomicInteger  num_msgs_sent;
        private int                  num_gets=0;
        private int                  num_puts=0;
        private Random random;


        public Invoker(Collection<Address> dests, int num_msgs_to_send, AtomicInteger num_msgs_sent, Random random) {
            this.num_msgs_sent=num_msgs_sent;
            this.dests.addAll(dests);
            this.num_msgs_to_send=num_msgs_to_send;
            this.random = random;
            setName("Invoker-" + COUNTER.getAndIncrement());
        }

        public int numGets() {return num_gets;}
        public int numPuts() {return num_puts;}


        public void run() {
        	System.out.println(" Inside run");
            final byte[] buf=new byte[msg_size];
            Object[] put_args={0, buf};
            Object[] get_args={0};
            MethodCall get_call=new MethodCall(GET, get_args);
            MethodCall put_call=new MethodCall(PUT, put_args);
            RequestOptions get_options=new RequestOptions(ResponseMode.GET_ALL, timeout, false, null);
            RequestOptions put_options=new RequestOptions(sync ? ResponseMode.GET_ALL : ResponseMode.GET_NONE, timeout, true, null);

            // Don't use bundling as we have sync requests (e.g. GETs) regardless of whether we set sync=true or false
            get_options.setFlags(Message.Flag.DONT_BUNDLE);
            put_options.setFlags(Message.Flag.DONT_BUNDLE);

            if(oob) {
                get_options.setFlags(Message.Flag.OOB);
                put_options.setFlags(Message.Flag.OOB);
            }
            if(sync) {
                get_options.setFlags(Message.Flag.DONT_BUNDLE, Message.NO_FC);
                put_options.setFlags(Message.Flag.DONT_BUNDLE, Message.NO_FC);
            }
            if(use_anycast_addrs) {
                put_options.useAnycastAddresses(true);
            }

            while(true) {
                long i=num_msgs_sent.getAndIncrement();
                if(i >= num_msgs_to_send){
                    break;
                }

                boolean get=Util.tossWeightedCoin(read_percentage);

                try {
                    if(get) { // sync GET
                        Address target=pickTarget();
                        get_args[0]=i;
                        disp.callRemoteMethod(target, get_call, get_options);
                        num_gets++;
                    }
                    else {    // sync or async (based on value of 'sync') PUT
                    	System.out.println("invoker name "+ getName());
                        Collection<Address> targets = random_destinations ? pickRandomAnycastTargets() : pickAnycastTargets();
                        put_args[0]=i;
                        RspList rsp = disp.callRemoteMethods(targets, put_call, put_options);
                        num_puts++;

                    }
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }

        private Address pickTarget() {
            int index=dests.indexOf(local_addr);
            int new_index=(index +1) % dests.size();
            return dests.get(new_index);
        }

        private Collection<Address> pickRandomAnycastTargets() {
            Collection<Address> anycastTargets = new HashSet<Address>(anycast_count);

            if (include_local_address)
                anycastTargets.add(local_addr);

            while (anycastTargets.size() < anycast_count) {
                int randomIndex = random.nextInt(dests.size());
                Address randomAddress = dests.get(randomIndex);

                if (!anycastTargets.contains(randomAddress))
                    anycastTargets.add(randomAddress);
            }
            return anycastTargets;
        }

        private Collection<Address> pickAnycastTargets() {
            Collection<Address> anycast_targets=new ArrayList<Address>(anycast_count);

            if (include_local_address)
                anycast_targets.add(local_addr);

            int index=dests.indexOf(local_addr);
            for(int i=index + 1; i < index + 1 + anycast_count; i++) {
                int new_index=i % dests.size();
                Address tmp=dests.get(new_index);
                if(!anycast_targets.contains(tmp))
                    anycast_targets.add(tmp);
            }
            return anycast_targets;
        }
    }


    public static class Results implements Streamable {
        long num_gets=0;
        long num_puts=0;
        long time=0;

        public Results() {

        }

        public Results(int num_gets, int num_puts, long time) {
            this.num_gets=num_gets;
            this.num_puts=num_puts;
            this.time=time;
        }




        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(num_gets);
            out.writeLong(num_puts);
            out.writeLong(time);
        }

        public void readFrom(DataInput in) throws Exception {
            num_gets=in.readLong();
            num_puts=in.readLong();
            time=in.readLong();
        }

        public String toString() {
            long total_reqs=num_gets + num_puts;
            double total_reqs_per_sec=total_reqs / (time / 1000.0);

            return f.format(total_reqs_per_sec) + " reqs/sec (" + num_gets + " GETs, " + num_puts + " PUTs total)";
        }
    }


    public static class ConfigOptions implements Streamable {
        private boolean sync, oob;
        private int     num_threads;
        private int     num_msgs, msg_size;
        private int     anycast_count;
        private boolean use_anycast_addrs;
        private double  read_percentage;

        public ConfigOptions() {
        }

        public ConfigOptions(boolean oob, boolean sync, int num_threads, int num_msgs, int msg_size,
                             int anycast_count, boolean use_anycast_addrs,
                             double read_percentage) {
            this.oob=oob;
            this.sync=sync;
            this.num_threads=num_threads;
            this.num_msgs=num_msgs;
            this.msg_size=msg_size;
            this.anycast_count=anycast_count;
            this.use_anycast_addrs=use_anycast_addrs;
            this.read_percentage=read_percentage;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeBoolean(oob);
            out.writeBoolean(sync);
            out.writeInt(num_threads);
            out.writeInt(num_msgs);
            out.writeInt(msg_size);
            out.writeInt(anycast_count);
            out.writeBoolean(use_anycast_addrs);
            out.writeDouble(read_percentage);
        }

        public void readFrom(DataInput in) throws Exception {
            oob=in.readBoolean();
            sync=in.readBoolean();
            num_threads=in.readInt();
            num_msgs=in.readInt();
            msg_size=in.readInt();
            anycast_count=in.readInt();
            use_anycast_addrs=in.readBoolean();
            read_percentage=in.readDouble();
        }

        public String toString() {
            return "oob=" + oob + ", sync=" + sync + ", anycast_count=" + anycast_count +
                    ", use_anycast_addrs=" + use_anycast_addrs +
                    ", num_threads=" + num_threads + ", num_msgs=" + num_msgs + ", msg_size=" + msg_size +
                    ", read percentage=" + read_percentage;
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
                case SET_USE_ANYCAST_ADDRS:
                    return new Buffer(booleanBuffer(call.getId(), (Boolean)call.getArgs()[0]));
                case SET_NUM_MSGS:
                case SET_NUM_THREADS:
                case SET_MSG_SIZE:
                case SET_ANYCAST_COUNT:
                    return new Buffer(intBuffer(call.getId(), (Integer)call.getArgs()[0]));
                case GET:
                    return new Buffer(longBuffer(call.getId(), (Long)call.getArgs()[0]));
                case PUT:
                    Long long_arg=(Long)call.getArgs()[0];
                    byte[] arg2=(byte[])call.getArgs()[1];
                    buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + Global.LONG_SIZE + arg2.length);
                    buf.put((byte)call.getId()).putLong(long_arg).putInt(arg2.length).put(arg2, 0, arg2.length);
                    return new Buffer(buf.array());
                case SET_READ_PERCENTAGE:
                    Double double_arg=(Double)call.getArgs()[0];
                    buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.DOUBLE_SIZE);
                    buf.put((byte)call.getId()).putDouble(double_arg);
                    return new Buffer(buf.array());
                default:
                    throw new IllegalStateException("method " + call.getMethod() + " not known");
            }
        }



        public Object objectFromBuffer(byte[] buffer, int offset, int length) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer, offset, length);

            byte type=buf.get();
            switch(type) {
                case START:
                case GET_CONFIG:
                    return new MethodCall(type);
                case SET_OOB:
                case SET_SYNC:
                case SET_USE_ANYCAST_ADDRS:
                    return new MethodCall(type, buf.get() == 1);
                case SET_NUM_MSGS:
                case SET_NUM_THREADS:
                case SET_MSG_SIZE:
                case SET_ANYCAST_COUNT:
                    return new MethodCall(type, buf.getInt());
                case GET:
                    return new MethodCall(type, buf.getLong());
                case PUT:
                    Long longarg=buf.getLong();
                    int len=buf.getInt();
                    byte[] arg2=new byte[len];
                    buf.get(arg2, 0, arg2.length);
                    return new MethodCall(type, longarg, arg2);
                case SET_READ_PERCENTAGE:
                    return new MethodCall(type, buf.getDouble());
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
            ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE *2);
            buf.put((byte)type).put((byte)(arg? 1 : 0));
            return buf.array();
        }
    }


    public static void main(String[] args) {
		String propsFile = "conf/sequencer.xml";
		String name ="ZAB";
		String outputDir= "/home/pg/p13/a6915654/"+name+"/";
		String [] zabboxInits= new String[3];
		List<String> zabMembers = new ArrayList<String>();
        int msgSize = 1000;
        int load = 1;
        int numsThreads= 10;
        int numberOfMessages= 100000; // #Msgs to be executed by this node
        int totalMessages= 1000000; // #Msgs to be sent by the whole cluster
        int numOfClients= 10; 
        int numWarmUp= 10000; 


        for (int i = 0; i < args.length; i++) {

             	 
        	if("-warmup".equals(args[i])) {
                numWarmUp = Integer.parseInt(args[++i]);
                System.out.println(numWarmUp);
                continue;
            }
        	
        	if("-load".equals(args[i])) {
                load = Integer.parseInt(args[++i]);
                System.out.println(load);
                continue;
            }
        	
        	if("-config".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
        	if ("-hosts".equals(args[i])){
				zabboxInits = args[++i].split(","); 
				zabMembers.addAll(Arrays.asList(zabboxInits));
		    }
        	 if("-name".equals(args[i])) {
                name = args[++i];
                continue;
            }
        	 if("-tmessages".equals(args[i])){
                totalMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-nmessages".equals(args[i])) {
                numberOfMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-threads".equals(args[i])) {
            	numsThreads = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-msgSize".equals(args[i])) {
            	msgSize = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-outputDir".equals(args[i])) {
            	outputDir = args[++i];
                continue;
            }
        	 if("-numClients".equals(args[i])) {
        		 numOfClients = Integer.parseInt(args[++i]);
                 continue;
             }
        	 
        }

        TestAll test=null;
        try {
            test=new TestAll();
            test.init(zabMembers, name, propsFile, totalMessages,
					numberOfMessages, numsThreads, msgSize, 
					outputDir, numOfClients, load, numWarmUp);
            test.eventLoop();
           
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UPerf [-props <props>] [-name name] [-xsite <true | false>] [-boxes String[]]");
    }


}