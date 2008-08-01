package org.jgroups.tests.perf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Version;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

/**  You start the test by running this class.
 * @author Bela Ban (belaban@yahoo.com)

 */
public class Test implements Receiver {
    String          props=null;
    Properties      config;
    boolean         sender=false;
    Transport       transport=null;
    Object          local_addr=null;

    /** Keys=member addresses, value=MemberInfo */
    final Map<Object,MemberInfo> senders=new ConcurrentHashMap<Object,MemberInfo>(10);

    /** Keeps track of members. ArrayList<SocketAddress> */
    final List      members=new ArrayList();


    /** Set when first message is received */
    long            start=0;

    /** Set when last message is received */
    long            stop=0;

    int             num_members=0;
    int             num_senders=0;

    int             num_buddies=0; // 0 == off, > 0 == on
    List            buddies=null;

    long            num_msgs_expected=0;

    long            num_msgs_received=0;  // from everyone
    long            num_bytes_received=0; // from everyone

    Log             log=LogFactory.getLog(getClass());

    boolean         all_received=false;
    boolean         final_results_received=false;

    /** Map<Object, MemberInfo>. A hashmap of senders, each value is the 'senders' hashmap */
    Map             results=new HashMap();

    private ResultsPublisher publisher=new ResultsPublisher();

    final List      heard_from=new ArrayList();

    final List      final_results_ok_list=new ArrayList();

    boolean         dump_transport_stats=false;

    /** Log every n msgs received */
    long            log_interval=1000;


    long            counter=1;
    long            msg_size=1000;
    boolean         jmx=false;

    /** Number of ms to wait at the receiver to simulate processing of the received message (0 == don't wait) */
    long            processing_delay=0;

    final           Set start_msgs_received=new HashSet(12);

    FileWriter      output=null;

    static  NumberFormat f;




    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }



    public void start(Properties c, boolean verbose, boolean jmx, String output, int num_threads, int num_buddies) throws Exception {
        String          config_file="config.txt";
        BufferedReader  fileReader;
        String          line;
        String          key, val;
        StringTokenizer st;
        Properties      tmp=new Properties();
        long num_msgs=0;

        if(output != null)
            this.output=new FileWriter(output, false);

        config_file=c.getProperty("config");
        fileReader=new BufferedReader(new FileReader(config_file));
        while((line=fileReader.readLine()) != null) {
            if(line.startsWith("#"))
                continue;
            line=line.trim();
            if(line.length() == 0)
                continue;
            st=new StringTokenizer(line, "=", false);
            key=st.nextToken().toLowerCase();
            val=st.nextToken();
            tmp.put(key, val);
        }
        fileReader.close();

        // 'tmp' now contains all properties from the file, now we need to override the ones
        // passed to us by 'c'
        tmp.putAll(c);
        this.config=tmp;

        num_msgs=Integer.parseInt(config.getProperty("num_msgs"));
        if(num_threads > 0 && num_msgs % num_threads != 0)
            throw new IllegalArgumentException("num_msgs (" + num_msgs + ") must be divisible by num_threads (" + num_threads + ")");

        StringBuilder sb=new StringBuilder();
        sb.append("\n\n----------------------- TEST -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n\n");
        if(verbose)
            sb.append("Properties: ").append(printProperties()).append("\n-------------------------\n\n");

        for(Iterator it=this.config.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(":\t").append(entry.getValue()).append('\n');
        }
        sb.append("JGroups version: ").append(Version.description).append('\n');
        System.out.println("Configuration is: " + sb);

        output(sb.toString());

        props=this.config.getProperty("props");
        num_members=Integer.parseInt(this.config.getProperty("num_members"));
        num_senders=Integer.parseInt(this.config.getProperty("num_senders"));
        this.num_buddies=Integer.parseInt(this.config.getProperty("num_buddies", "0"));
        if(num_buddies > 0)
            this.num_buddies=num_buddies;
        if(num_buddies > num_members -1) {
            throw new IllegalArgumentException("num_buddes (" + num_buddies + " cannot be greater than num_members -1 ("
                    + (num_members -1) + ")");
        }
        num_msgs=Long.parseLong(this.config.getProperty("num_msgs"));
        this.num_msgs_expected=num_senders * num_msgs;
        sender=Boolean.valueOf(this.config.getProperty("sender")).booleanValue();
        msg_size=Long.parseLong(this.config.getProperty("msg_size"));
        String tmp2=this.config.getProperty("dump_transport_stats", "false");
        if(Boolean.valueOf(tmp2).booleanValue())
            this.dump_transport_stats=true;
        tmp2=this.config.getProperty("log_interval");
        if(tmp2 != null)
            log_interval=Long.parseLong(tmp2);

        if(num_threads > 0 && log_interval % num_threads != 0)
            throw new IllegalArgumentException("log_interval (" + log_interval + ") must be divisible by num_threads (" + num_threads + ")");

        sb=new StringBuilder();
        sb.append("\n##### msgs_received");
        sb.append(", current time (in ms)");
        sb.append(", msgs/sec");
        sb.append(", throughput/sec [KB]");
        sb.append(", free_mem [KB] ");
        sb.append(", total_mem [KB] ");
        output(sb.toString());

        if(jmx) {
            this.config.setProperty("jmx", "true");
        }
        this.jmx=Boolean.valueOf(this.config.getProperty("jmx")).booleanValue();

        String tmp3=this.config.getProperty("processing_delay");
        if(tmp3 != null)
            this.processing_delay=Long.parseLong(tmp3);


        String transport_name=this.config.getProperty("transport");
        transport=(Transport)Util.loadClass(transport_name, this.getClass()).newInstance();
        transport.create(this.config);
        transport.setReceiver(this);
        transport.start();
        local_addr=transport.getLocalAddress();
    }

    private void output(String msg) {
        // if(log.isInfoEnabled())
           // log.info(msg);
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
        if(transport != null) {
            transport.stop();
            transport.destroy();
        }
        if(this.output != null) {
            try {
                this.output.close();
            }
            catch(IOException e) {
            }
        }
    }

    public void receive(Object sender, byte[] payload) {
        if(payload == null || payload.length == 0) {
            System.err.println("payload is incorrect (sender=" + sender + ")");
            return;
        }

        try {
            int type=payload[0];
            if(type == 1) { // DATA
                int len=payload.length -1;
                handleData(sender, len);
                return;
            }

            byte[] tmp=new byte[payload.length-1];
            System.arraycopy(payload, 1, tmp, 0, tmp.length);
            Data d=(Data)Util.streamableFromByteBuffer(Data.class, tmp);

            switch(d.getType()) {
                case Data.DISCOVERY_REQ:
                    sendDiscoveryResponse();
                    break;
                case Data.DISCOVERY_RSP:
                    synchronized(this.members) {
                        if(!this.members.contains(sender)) {
                            this.members.add(sender);
                            System.out.println("-- " + sender + " joined");
                            if(d.sender) {
                                if(!this.senders.containsKey(sender)) {
                                    this.senders.put(sender, new MemberInfo(d.num_msgs));
                                }
                            }
                            this.members.notifyAll();
                            if(num_buddies > 0)
                                computeBuddies();
                        }
                    }
                    break;

                case Data.RESULTS:
                    results.put(sender, d.result);
                    heard_from.remove(sender);
                    if(heard_from.isEmpty()) {
                        for(int i=0; i < 3; i++) {
                            sendFinalResults();
                            Util.sleep(300);
                        }
                    }
                    break;

                case Data.FINAL_RESULTS:
                    if(!final_results_received) {
                        dumpResults(d.results);
                        final_results_received=true;
                    }

                    boolean done=false;
                    synchronized(final_results_ok_list) {
                        final_results_ok_list.remove(sender);
                        if(final_results_ok_list.isEmpty())
                            done=true;
                    }

                    if(done) {
                        for(int i=0; i < 3; i++) {
                            sendFinalResultsOk();
                            Util.sleep(300);
                        }
                    }
                    break;

                case Data.FINAL_RESULTS_OK:
                    publisher.stop();
                    synchronized(this) {
                        this.notifyAll();
                    }
                    break;

                case Data.START:
                    synchronized(start_msgs_received) {
                        if(start_msgs_received.add(sender)){
                            start_msgs_received.notifyAll();
                        }
                    }
                    break;

                default:
                    log.error("received invalid data type: " + payload[0]);
                    break;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void computeBuddies() {

    }

    private void handleData(Object sender, int num_bytes) {
        MemberInfo info=null;
        boolean do_sleep=false;

        synchronized(this) {
            if(all_received)
                return;
            if(start == 0) {
                start=System.currentTimeMillis();
            }

            num_msgs_received++;
            num_bytes_received+=num_bytes;

            if(num_msgs_received >= num_msgs_expected) {
                if(stop == 0) {
                    stop=System.currentTimeMillis();
                }
                all_received=true;
            }

            if(num_msgs_received % log_interval == 0) {
                System.out.println(new StringBuilder("-- received ").append(num_msgs_received).append(" messages"));
            }

            if(counter % log_interval == 0) {
                output(dumpStats(counter));
            }
            info=this.senders.get(sender);
            if(info != null) {
                if(info.start == 0)
                    info.start=System.currentTimeMillis();
                info.num_msgs_received++;
                counter++;
                info.total_bytes_received+=num_bytes;
                if(info.num_msgs_received >= info.num_msgs_expected) {
                    info.done=true;
                    if(info.stop == 0)
                        info.stop=System.currentTimeMillis();
                }
                else {
                    if(processing_delay > 0)
                        do_sleep=true;
                }
            }
            else {
                log.error("-- sender " + sender + " not found in senders hashmap");
            }
        }

        if(do_sleep && processing_delay > 0) {
            Util.sleep(processing_delay);
        }

        synchronized(this) {
            if(all_received) {
                if(!this.sender)
                    dumpSenders();
                publisher.start();
            }
        }
    }

    private void sendResults() throws Exception {
        Data d=new Data(Data.RESULTS);
        byte[] buf;
        MemberInfo info=new MemberInfo(num_msgs_expected);
        info.done=true;
        info.num_msgs_received=num_msgs_received;
        info.start=start;
        info.stop=stop;
        info.total_bytes_received=this.num_bytes_received;
        d.result=info;
        buf=generatePayload(d, null);
        transport.send(null, buf, true);
    }


    private void sendFinalResults() throws Exception {
        Data d=new Data(Data.FINAL_RESULTS);
        d.results=new ConcurrentHashMap<Object,MemberInfo>(this.results);
        final byte[] buf=generatePayload(d, null);
        transport.send(null, buf, true);
    }

    private void sendFinalResultsOk() throws Exception {
        Data d=new Data(Data.FINAL_RESULTS_OK);
        final byte[] buf=generatePayload(d, null);
        transport.send(null, buf, true);
    }

    boolean allReceived() {
        return all_received;
    }

    boolean receivedFinalResults() {
        return final_results_received;
    }


    void sendMessages(long interval, int nanos, boolean busy_sleep, int num_threads) throws Exception {
        long total_msgs=0;
        int msgSize=Integer.parseInt(config.getProperty("msg_size"));
        int num_msgs=Integer.parseInt(config.getProperty("num_msgs"));
        byte[] buf=new byte[msgSize];
        for(int k=0; k < msgSize; k++)
            buf[k]='.';
        Data d=new Data(Data.DATA);
        byte[] payload=generatePayload(d, buf);
        StringBuilder sb=new StringBuilder();
        sb.append("-- sending ").append(num_msgs).append(" ").append(Util.printBytes(msgSize)).append(" messages");
        if(num_threads > 0)
            sb.append(" in ").append(num_threads).append(" threads");
        System.out.println(sb);

        if(num_threads > 0) {
            int number=num_msgs / num_threads;
            long thread_interval=log_interval / num_threads;
            CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
            Thread[] threads=new Thread[num_threads];
            for(int i=0; i < threads.length; i++) {
                threads[i]=new Sender(barrier, number, payload, interval, nanos, busy_sleep, thread_interval);
                threads[i].setName("thread-" + (i+1));
                threads[i].start();
            }
            barrier.await();
        }
        else {
            for(int i=0; i < num_msgs; i++) {
                transport.send(null, payload, false);
                total_msgs++;
                if(total_msgs % log_interval == 0) {
                    System.out.println("++ sent " + total_msgs);
                }
                if(interval > 0 || nanos > 0) {
                    if(busy_sleep)
                        Util.sleep(interval, busy_sleep);
                    else
                        Util.sleep(interval, nanos);
                }
            }
        }
    }


    class Sender extends Thread {
        CyclicBarrier barrier;
        int num_msgs=0;
        byte[] payload;
        long total_msgs=0;
        long interval=0;
        boolean busy_sleep=false;
        int nanos=0;
        long thread_interval;

        Sender(CyclicBarrier barrier, int num_msgs, byte[] payload, long interval, int nanos, boolean busy_sleep, long thread_interval) {
            this.barrier=barrier;
            this.num_msgs=num_msgs;
            this.payload=payload;
            this.interval=interval;
            this.nanos=nanos;
            this.busy_sleep=busy_sleep;
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
                    transport.send(null, payload, false);
                    total_msgs++;
                    if(total_msgs % log_interval == 0) {
                        System.out.println("++ sent " + total_msgs + " [" + getName() + "]");
                    }
                    if(interval > 0 || nanos > 0) {
                        if(busy_sleep)
                            Util.sleep(interval, busy_sleep);
                        else
                            Util.sleep(interval, nanos);
                    }
                }
                catch(Exception e) {
                }
            }
        }
    }



    static byte[] generatePayload(Data d, byte[] buf) throws Exception {
        byte[] tmp=buf != null? buf : Util.streamableToByteBuffer(d);
        byte[] payload=new byte[tmp.length +1];
        payload[0]=intToByte(d.getType());
        System.arraycopy(tmp, 0, payload, 1, tmp.length);
        return payload;
    }

    private static byte intToByte(int type) {
        switch(type) {
            case Data.DATA: return 1;
            case Data.DISCOVERY_REQ: return 2;
            case Data.DISCOVERY_RSP: return 3;
            case Data.RESULTS: return 4;
            case Data.FINAL_RESULTS: return 5;
            default: return 0;
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


    private void dumpSenders() {
        StringBuilder sb=new StringBuilder();
        dump(this.senders, sb);
        System.out.println(sb.toString());
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

        msgs_sec=received_msgs / ((current - start) / 1000.0);
        throughput_sec=msgs_sec * msg_size;

        sb.append(f.format(msgs_sec)).append(' ').append(f.format(throughput_sec)).append(' ');

        sb.append(Runtime.getRuntime().freeMemory() / 1000.0).append(' ');

        sb.append(Runtime.getRuntime().totalMemory() / 1000.0);


        if(dump_transport_stats) {
            Map stats=transport.dumpStats();
            if(stats != null) {
                print(stats, sb);
            }
        }
        return sb.toString();
    }


    public String dumpTransportStats() {
        Map stats=transport.dumpStats();
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


    void runDiscoveryPhase() throws Exception {
        sendDiscoveryRequest();
        sendDiscoveryResponse();

        System.out.println("-- waiting for " + num_members + " members to join");
        boolean received_all_discovery_rsps=false;
        while(!received_all_discovery_rsps) {
            synchronized(members) {
                received_all_discovery_rsps=members.size() >= num_members;
                if(!received_all_discovery_rsps)
                    members.wait(2000);
            }
            if(!received_all_discovery_rsps) {
                sendDiscoveryRequest();
                sendDiscoveryResponse();
            }
        }

        synchronized(members) {
            heard_from.addAll(members);
            // System.out.println("-- members: " + members.size());
        }

        synchronized(final_results_ok_list) {
            final_results_ok_list.addAll(members);
        }
    }


    void waitForAllOKs() throws Exception {
        sendStarted();
        boolean received_all_start_msgs=false;
        while(!received_all_start_msgs) {
            synchronized(start_msgs_received) {
                received_all_start_msgs=start_msgs_received.size() >= num_members;
                if(!received_all_start_msgs)
                    start_msgs_received.wait(2000);
            }
            if(!received_all_start_msgs) {
                sendStarted();
            }
        }
        System.out.println("-- READY (" + start_msgs_received.size() + " acks)\n");
    }

    void sendStarted() throws Exception {
        transport.send(null, generatePayload(new Data(Data.START), null), true);
    }

    void sendDiscoveryRequest() throws Exception {
        Data d=new Data(Data.DISCOVERY_REQ);
        // System.out.println("-- sending discovery request");
        transport.send(null, generatePayload(d, null), true);
    }

    void sendDiscoveryResponse() throws Exception {
        final Data d2=new Data(Data.DISCOVERY_RSP);
        if(sender) {
            d2.sender=true;
            d2.num_msgs=Long.parseLong(config.getProperty("num_msgs"));
        }

        transport.send(null, generatePayload(d2, null), true);
    }


    public static void main(String[] args) {
        Properties config=new Properties();
        boolean sender=false, verbose=false, jmx=false, dump_stats=false; // dumps at end of run
        Test t=null;
        String output=null;
        long interval=0;
        int interval_nanos=0;
        boolean busy_sleep=false;
        int num_threads=0;
        int num_buddies=0; // 0 == off, > 0 == enabled

        for(int i=0; i < args.length; i++) {
            if("-bind_addr".equals(args[i])) {
                String bind_addr=args[++i];
                System.setProperty("jgroups.bind_addr", bind_addr);
                continue;
            }
            if("-sender".equals(args[i])) {
                config.put("sender", "true");
                sender=true;
                continue;
            }
            if("-receiver".equals(args[i])) {
                config.put("sender", "false");
                sender=false;
                continue;
            }
            if("-config".equals(args[i])) {
                String config_file=args[++i];
                config.put("config", config_file);
                continue;
            }
            if("-props".equals(args[i])) {
                String props=args[++i];
                config.put("props", props);
                continue;
            }
            if("-verbose".equals(args[i])) {
                verbose=true;
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            if("-dump_stats".equals(args[i])) {
                dump_stats=true;
                continue;
            }
            if("-num_threads".equals(args[i])) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if("-interval".equals(args[i])) {
                interval=Long.parseLong(args[++i]);
                continue;
            }
            if("-nanos".equals(args[i])) {
                interval_nanos=Integer.parseInt(args[++i]);
                continue;
            }
            if("-busy_sleep".equals(args[i])) {
                busy_sleep=true;
                continue;
            }
            if("-f".equals(args[i])) {
                output=args[++i];
                continue;
            }
            if("-num_buddies".equals(args[i])) {
                num_buddies=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

  
        try {
            t=new Test();
            t.start(config, verbose, jmx, output, num_threads, num_buddies);
            t.runDiscoveryPhase();
            t.waitForAllOKs();
            if(sender) {
                t.sendMessages(interval, interval_nanos, busy_sleep, num_threads);
            }
            synchronized(t) {
                while(t.receivedFinalResults() == false) {
                    t.wait(2000);
                }
            }
            if(dump_stats) {
                String stats=t.dumpTransportStats();
                System.out.println("\nTransport statistics:\n" + stats);
            }

            if(t.jmx) {
                System.out.println("jmx=true: not terminating");
                if(t != null) {
                    t.stop();
                    t=null;
                }
                while(true) {
                    Util.sleep(60000);
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if(t != null) {
                t.stop();
            }
        }
    }


    static void help() {
        System.out.println("Test [-help] ([-sender] | [-receiver]) " +
                "[-config <config file>] [-num_threads <number of threads for sending messages>]" +
                "[-props <stack config>] [-verbose] [-jmx] [-bind_addr <bind address>" +
                "[-dump_stats] [-f <filename>] [-interval <ms between sends>] " +
                "[-nanos <additional nanos to sleep in interval>] [-busy_sleep (cancels out -nanos)] " +
                "[-num_buddies <number of backup buddies>, this enables buddy replication]");
    }


    private class ResultsPublisher implements Runnable {
        static final long interval=1000;
        boolean running=true;
        volatile Thread t;

        void start() {
            if(t == null) {
                t=new Thread(this, "ResultsPublisher");
                t.setDaemon(true);
                t.start();
            }
        }

        void stop() {
            if(t != null && t.isAlive()) {
                Thread tmp=t;
                t=null;
                if(tmp != null)
                    tmp.interrupt();
            }
        }

        public void run() {
            try {
                while(t != null) {
                    sendResults();
                    Util.sleep(interval);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }


}
