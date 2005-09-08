package org.jgroups.tests.perf;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Version;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.text.NumberFormat;

/**  You start the test by running this class.
 * @author Bela Ban (belaban@yahoo.com)

 */
public class Test implements Receiver {
    String          props=null;
    Properties      config;
    boolean         sender=false;
    Transport       transport=null;
    Object          local_addr=null;

    /** Map<Object,MemberInfo> members. Keys=member addresses, value=MemberInfo */
    Map             senders=new ConcurrentReaderHashMap(10);

    /** Keeps track of members. ArrayList<SocketAddress> */
    ArrayList       members=new ArrayList();


    /** Set when first message is received */
    long            start=0;

    /** Set when last message is received */
    long            stop=0;

    int             num_members=0;
    int             num_senders=0;
    long            num_msgs_expected=0;

    long            num_msgs_received=0;  // from everyone
    long            num_bytes_received=0; // from everyone

    Log             log=LogFactory.getLog(getClass());

    boolean         all_received=false;
    boolean         final_results_received=false;

    /** Map<Object, MemberInfo>. A hashmap of senders, each value is the 'senders' hashmap */
    Map             results=new HashMap();

    ResultsPublisher publisher=new ResultsPublisher();

    List            heard_from=new ArrayList();

    /** Dump info in gnuplot format */
    boolean         gnuplot_output=false;

    boolean         dump_transport_stats=false;

    /** Log every n msgs received (if gnuplot_output == true) */
    long            log_interval=1000;

    /** Last time we dumped info */
    long            last_dump=0;

    long            counter=1;
    long            msg_size=1000;
    boolean         jmx=false;

    transient static  NumberFormat f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }



    public void start(Properties c, boolean verbose, boolean jmx) throws Exception {
        String          config_file="config.txt";
        BufferedReader  fileReader;
        String          line;
        String          key, val;
        StringTokenizer st;
        Properties      tmp=new Properties();

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

        StringBuffer sb=new StringBuffer();
        sb.append("\n\n----------------------- TEST -----------------------\n");
        sb.append("Date: ").append(new Date()).append('\n');
        sb.append("Run by: ").append(System.getProperty("user.name")).append("\n\n");
        if(verbose)
            sb.append("Properties: ").append(printProperties()).append("\n-------------------------\n\n");

        for(Iterator it=this.config.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(":\t").append(entry.getValue()).append('\n');
        }
        sb.append("JGroups version: ").append(Version.printVersion()).append('\n');
        System.out.println("Configuration is: " + sb);

        log.info(sb.toString());

        props=this.config.getProperty("props");
        num_members=Integer.parseInt(this.config.getProperty("num_members"));
        num_senders=Integer.parseInt(this.config.getProperty("num_senders"));
        long num_msgs=Long.parseLong(this.config.getProperty("num_msgs"));
        this.num_msgs_expected=num_senders * num_msgs;
        sender=Boolean.valueOf(this.config.getProperty("sender")).booleanValue();
        msg_size=Long.parseLong(this.config.getProperty("msg_size"));
        String tmp2=this.config.getProperty("gnuplot_output", "false");
        if(Boolean.valueOf(tmp2).booleanValue())
            this.gnuplot_output=true;
        tmp2=this.config.getProperty("dump_transport_stats", "false");
        if(Boolean.valueOf(tmp2).booleanValue())
            this.dump_transport_stats=true;
        tmp2=this.config.getProperty("log_interval");
        if(tmp2 != null)
            log_interval=Long.parseLong(tmp2);

        if(gnuplot_output) {
            sb=new StringBuffer();
            sb.append("\n##### msgs_received");
            sb.append(", free_mem [KB] ");
            sb.append(", total_mem [KB] ");
            sb.append(", total_msgs_sec [msgs/sec] ");
            sb.append(", total_throughput [KB/sec] ");
            sb.append(", rolling_msgs_sec (last ").append(log_interval).append(" msgs) ");
            sb.append(" [msgs/sec] ");
            sb.append(", rolling_throughput (last ").append(log_interval).append(" msgs) ");
            sb.append(" [KB/sec]\n");
            if(log.isInfoEnabled()) log.info(sb.toString());
        }
        if(jmx) {
            this.config.setProperty("jmx", "true");
        }
        this.jmx=new Boolean(this.config.getProperty("jmx")).booleanValue();
        String transport_name=this.config.getProperty("transport");
        transport=(Transport)Util.loadClass(transport_name, this.getClass()).newInstance();
        transport.create(this.config);
        transport.setReceiver(this);
        transport.start();
        local_addr=transport.getLocalAddress();
    }

    private String printProperties() {
        StringBuffer sb=new StringBuffer();
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
    }

    public void receive(Object sender, byte[] payload) {
        if(payload == null || payload.length == 0) {
            System.err.println("payload is incorrect (sender=" + sender + "): " + payload);
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
                // System.out.println("-- received discovery request");
                sendDiscoveryResponse();
                break;
            case Data.DISCOVERY_RSP:
                // System.out.println("-- received discovery response from " + sender);
                synchronized(this.members) {
                    if(!this.members.contains(sender)) {
                        this.members.add(sender);
                        System.out.println("-- " + sender + " joined");
                        if(d.sender) {
                            synchronized(this.members) {
                                if(!this.senders.containsKey(sender)) {
                                    this.senders.put(sender, new MemberInfo(d.num_msgs));
                                }
                            }
                        }
                        this.members.notify();
                    }
                }
                break;

            case Data.FINAL_RESULTS:
                publisher.stop();
                if(!final_results_received) {
                    dumpResults(d.results);
                    final_results_received=true;
                }
                synchronized(this) {
                    this.notify();
                }
                break;

            case Data.RESULTS:
                results.put(sender, d.result);
                heard_from.remove(sender);
                if(heard_from.size() == 0) {
                    sendFinalResults();
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

    private void handleData(Object sender, int num_bytes) {
        if(all_received)
            return;
        if(start == 0) {
            start=System.currentTimeMillis();
            last_dump=start;
        }

        num_msgs_received++;
        num_bytes_received+=num_bytes;

        if(num_msgs_received >= num_msgs_expected) {
            if(stop == 0)
                stop=System.currentTimeMillis();
            all_received=true;
        }

        if(num_msgs_received % log_interval == 0)
            System.out.println(new StringBuffer("-- received ").append(num_msgs_received).append(" messages"));

        if(counter % log_interval == 0) {
            if(log.isInfoEnabled()) log.info(dumpStats(counter));
        }

        MemberInfo info=(MemberInfo)this.senders.get(sender);
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
        }
        else {
            log.error("-- sender " + sender + " not found in senders hashmap");
        }

        if(all_received) {
            if(!this.sender)
                dumpSenders();
            publisher.start();
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
        transport.send(null, buf);
    }


    private void sendFinalResults() throws Exception {
        Data d=new Data(Data.FINAL_RESULTS);
        byte[] buf;
        d.results=new ConcurrentReaderHashMap(this.results);
        buf=generatePayload(d, null);
        transport.send(null, buf);
    }

    boolean allReceived() {
        return all_received;
    }


    void sendMessages() throws Exception {
        long total_msgs=0;
        int msgSize=Integer.parseInt(config.getProperty("msg_size"));
        int num_msgs=Integer.parseInt(config.getProperty("num_msgs"));
        // int logInterval=Integer.parseInt(config.getProperty("log_interval"));
        // boolean gnuplot_output=Boolean.getBoolean(config.getProperty("gnuplot_output", "false"));
        byte[] buf=new byte[msgSize];
        for(int k=0; k < msgSize; k++)
            buf[k]='.';
        Data d=new Data(Data.DATA);
        byte[] payload=generatePayload(d, buf);
        for(int i=0; i < num_msgs; i++) {
            transport.send(null, payload);
            total_msgs++;
            if(total_msgs % log_interval == 0) {
                System.out.println("++ sent " + total_msgs);
            }
        }
    }


    byte[] generatePayload(Data d, byte[] buf) throws Exception {
        byte[] tmp=buf != null? buf : Util.streamableToByteBuffer(d);
        byte[] payload=new byte[tmp.length +1];
        payload[0]=intToByte(d.getType());
        System.arraycopy(tmp, 0, payload, 1, tmp.length);
        return payload;
    }

    private byte intToByte(int type) {
        switch(type) {
            case Data.DATA: return 1;
            case Data.DISCOVERY_REQ: return 2;
            case Data.DISCOVERY_RSP: return 3;
            case Data.RESULTS: return 4;
            case Data.FINAL_RESULTS: return 5;
            default: return 0;
        }
    }



//    private void dumpResults(Map final_results) {
//        Object      member;
//        Map.Entry   entry;
//        Map         map;
//        StringBuffer sb=new StringBuffer();
//        sb.append("\n-- results:\n\n");
//
//        for(Iterator it=final_results.entrySet().iterator(); it.hasNext();) {
//            entry=(Map.Entry)it.next();
//            member=entry.getKey();
//            map=(Map)entry.getValue();
//            sb.append("-- results from ").append(member);
//            if(member.equals(local_addr))
//                sb.append(" (myself)");
//            sb.append(":\n");
//            dump(map, sb);
//            sb.append('\n');
//        }
//        System.out.println(sb.toString());
//        if(log.isInfoEnabled()) log.info(sb.toString());
//    }


    private void dumpResults(Map final_results) {
        Object      member;
        Map.Entry   entry;
        MemberInfo  val;
        double      combined_msgs_sec, tmp=0;
        StringBuffer sb=new StringBuffer();
        sb.append("\n-- results:\n");

        for(Iterator it=final_results.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            member=entry.getKey();
            val=(MemberInfo)entry.getValue();
            tmp+=val.getMessageSec();
            sb.append("\n").append(member);
            if(member.equals(local_addr))
                sb.append(" (myself)");
            sb.append(":\n");
            sb.append(val);
            sb.append('\n');
        }
        combined_msgs_sec=tmp / final_results.size();
        sb.append("\ncombined: ").append(f.format(combined_msgs_sec)).
                append(" msgs/sec averaged over all receivers\n");
        System.out.println(sb.toString());
        if(log.isInfoEnabled()) log.info(sb.toString());
    }


    private void dumpSenders() {
        StringBuffer sb=new StringBuffer();
        dump(this.senders, sb);
        System.out.println(sb.toString());
    }

    private void dump(Map map, StringBuffer sb) {
        Map.Entry  entry;
        Object     mySender;
        MemberInfo mi;
        MemberInfo combined=new MemberInfo(0);
        combined.start = Long.MAX_VALUE;
        combined.stop = Long.MIN_VALUE;

        sb.append("\n-- local results:\n");
        for(Iterator it2=map.entrySet().iterator(); it2.hasNext();) {
            entry=(Map.Entry)it2.next();
            mySender=entry.getKey();
            mi=(MemberInfo)entry.getValue();
            combined.start=Math.min(combined.start, mi.start);
            combined.stop=Math.max(combined.stop, mi.stop);
            combined.num_msgs_expected+=mi.num_msgs_expected;
            combined.num_msgs_received+=mi.num_msgs_received;
            combined.total_bytes_received+=mi.total_bytes_received;
            sb.append("sender: ").append(mySender).append(": ").append(mi).append('\n');
        }
    }


    private String dumpStats(long received_msgs) {
        StringBuffer sb=new StringBuffer();
        if(gnuplot_output)
            sb.append(received_msgs).append(' ');
        else
            sb.append("\nmsgs_received=").append(received_msgs);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().freeMemory() / 1000.0).append(' ');
        else
            sb.append(", free_mem=").append(Runtime.getRuntime().freeMemory() / 1000.0);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().totalMemory() / 1000.0).append(' ');
        else
            sb.append(", total_mem=").append(Runtime.getRuntime().totalMemory() / 1000.0).append('\n');

        dumpThroughput(sb, received_msgs);

        if(dump_transport_stats) {
            Map stats=transport.dumpStats();
            if(stats != null) {
                print(stats, sb);
            }
        }
        return sb.toString();
    }

    private void print(Map stats, StringBuffer sb) {
        sb.append("\ntransport stats:\n");
        Map.Entry entry;
        Object key, val;
        for(Iterator it=stats.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }
    }

    private void dumpThroughput(StringBuffer sb, long received_msgs) {
        double tmp;
        long   current=System.currentTimeMillis();

        if(current - start == 0 || current - last_dump == 0)
            return;

        tmp=(1000 * received_msgs) / (current - start);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else
            sb.append("total_msgs_sec=").append(tmp).append(" [msgs/sec]");

        tmp=(received_msgs * msg_size) / (current - start);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else
            sb.append("\ntotal_throughput=").append(tmp).append(" [KB/sec]");

        tmp=(1000 * log_interval) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else {
            sb.append("\nrolling_msgs_sec (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [msgs/sec]");
        }

        tmp=(log_interval * msg_size) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else {
            sb.append("\nrolling_throughput (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [KB/sec]\n");
        }
        last_dump=current;
    }


    void runDiscoveryPhase() throws Exception {
        sendDiscoveryRequest();
        sendDiscoveryResponse();

        synchronized(this.members) {
            System.out.println("-- waiting for " + num_members + " members to join");
            while(this.members.size() < num_members) {
                this.members.wait(2000);
                sendDiscoveryRequest();
                sendDiscoveryResponse();
            }
            heard_from.addAll(members);
            System.out.println("-- members: " + this.members.size());
        }
    }

    void sendDiscoveryRequest() throws Exception {
        Data d=new Data(Data.DISCOVERY_REQ);
        // System.out.println("-- sending discovery request");
        transport.send(null, generatePayload(d, null));
    }

    void sendDiscoveryResponse() throws Exception {
        Data d2=new Data(Data.DISCOVERY_RSP);
        if(sender) {
            d2.sender=true;
            d2.num_msgs=Long.parseLong(config.getProperty("num_msgs"));
        }
        // System.out.println("-- sending discovery response");
        transport.send(null, generatePayload(d2, null));
    }


    public static void main(String[] args) {
        Properties config=new Properties();
        boolean sender=false, verbose=false, jmx=false;
        Test t=null;

        for(int i=0; i < args.length; i++) {
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
            help();
            return;
        }

        try {
            t=new Test();
            t.start(config, verbose, jmx);
            t.runDiscoveryPhase();
            if(sender) {
                t.sendMessages();
            }
            synchronized(t) {
                int i=0;
                while(t.allReceived() == false) {
                    t.wait(2000);
//                    i++;
//                    if(i > 5 && i % 10 == 0) {
//                        t.dumpSenders();
//                    }
                }
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
                           "[-config <config file>] [-props <stack config>] [-verbose] [-jmx]");
    }


    private class ResultsPublisher implements Runnable {
        final long interval=1000;
        boolean running=true;
        Thread t;

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
