package org.jgroups.tests.perf;

import org.apache.log4j.Logger;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;

/**  You start the test by running this class.
 * Use parameters -Xbatch -Xconcurrentio (Solaris specific)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class Test implements Receiver {
    String          props=null;
    Properties      config;
    boolean         sender=false;
    Transport       transport=null;
    Object          local_addr=null;

    /** HashMap<Object,MemberInfo> members. Keys=member addresses, value=MemberInfo */
    HashMap         senders=new HashMap();

    /** Keeps track of members */
    ArrayList       members=new ArrayList();

    /** Set when first message is received */
    long            start=0;

    /** Set when last message is received */
    long            stop=0;

    int             num_members=0;

    Logger          log=Logger.getLogger(this.getClass());

    boolean         all_received=false;

    /** HashMap<Object, HashMap>. A hashmap of senders, each value is the 'senders' hashmap */
    HashMap         results=new HashMap();



    public void start(Properties c) throws Exception {
        String          config_file="config.txt";
        BufferedReader  fileReader;
        String          line;
        String          key, val;
        StringTokenizer st;
        Properties      tmp=new Properties();

        Trace.init();

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
        for(Iterator it=this.config.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(":\t").append(entry.getValue()).append("\n");
        }
        sb.append("\n");
        System.out.println("Configuration is: " + sb.toString());
        Logger.getLogger(Test.class).info("main(): " + sb.toString());

        props=this.config.getProperty("props");
        num_members=Integer.parseInt(this.config.getProperty("num_members"));
        sender=new Boolean(this.config.getProperty("sender")).booleanValue();

        String transport_name=this.config.getProperty("transport");
        transport=(Transport)Thread.currentThread().getContextClassLoader().loadClass(transport_name).newInstance();
        transport.create(this.config);
        transport.setReceiver(this);
        transport.start();
        local_addr=transport.getLocalAddress();
    }

    public void stop() {
        if(transport != null) {
            transport.stop();
            transport.destroy();
        }
    }

    public void receive(Object sender, byte[] payload) {
        Data d;

        try {
            d=(Data)Util.objectFromByteBuffer(payload);

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

                case Data.DATA:
                    if(all_received)
                        return;
                    if(start == 0)
                        start=System.currentTimeMillis();
                    MemberInfo info=(MemberInfo)this.senders.get(sender);
                    if(info != null) {
                        if(info.start == 0)
                            info.start=System.currentTimeMillis();
                        info.num_msgs_received++;
                        info.total_bytes_received+=d.payload.length;
                        if(info.num_msgs_received % 1000 == 0)
                            System.out.println("-- received " + info.num_msgs_received +
                                    " messages from " + sender);
                        if(info.num_msgs_received >= info.num_msgs_expected) {
                            info.done=true;
                            if(info.stop == 0)
                                info.stop=System.currentTimeMillis();
                            if(allReceived()) {
                                all_received=true;
                                if(stop == 0)
                                    stop=System.currentTimeMillis();
                                sendResults();
                                if(!this.sender)
                                    dumpSenders();
                                synchronized(this) {
                                    this.notify();
                                }
                            }
                        }
                    }
                    else {
                        System.err.println("-- sender " + sender + " not found in senders hashmap");
                    }
                    break;

                case Data.DONE:
                    if(all_received)
                        return;
                    MemberInfo mi=(MemberInfo)this.senders.get(sender);
                    if(mi != null) {
                        mi.done=true;
                        if(mi.stop == 0)
                            mi.stop=System.currentTimeMillis();
                        if(allReceived()) {
                            all_received=true;
                            if(stop == 0)
                                stop=System.currentTimeMillis();
                            sendResults();
                            if(!this.sender)
                                dumpSenders();
                            synchronized(this) {
                                this.notify();
                            }
                        }
                    }
                    else {
                        System.err.println("-- sender " + sender + " not found in senders hashmap");
                    }
                    break;

                case Data.RESULTS:
                    synchronized(results) {
                        if(!results.containsKey(sender)) {
                            results.put(sender, d.results);
                            results.notify();
                        }
                    }
                    break;

                default:
                    System.err.println("received invalid data type: " + payload[0]);
                    break;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    void sendResults() throws Exception {
        Data d=new Data(Data.RESULTS);
        byte[] buf;
        d.results=(HashMap)this.senders.clone();
        buf=Util.objectToByteBuffer(d);
        transport.send(null, buf);
    }

    boolean allReceived() {
        MemberInfo mi;

        for(Iterator it=this.senders.values().iterator(); it.hasNext();) {
            mi=(MemberInfo)it.next();
            if(mi.done == false)
                return false;
        }
        return true;
    }


    void sendMessages() throws Exception {
        long total_msgs=0;
        int msg_size=Integer.parseInt(config.getProperty("msg_size"));
        int num_msgs=Integer.parseInt(config.getProperty("num_msgs"));
        int log_interval=Integer.parseInt(config.getProperty("log_interval"));
        // boolean gnuplot_output=Boolean.getBoolean(config.getProperty("gnuplot_output", "false"));
        byte[] buf=new byte[msg_size];
        for(int k=0; k < msg_size; k++)
            buf[k]='.';
        Data d=new Data(Data.DATA);
        d.payload=buf;
        byte[] payload=Util.objectToByteBuffer(d);

        for(int i=0; i < num_msgs; i++) {
            transport.send(null, payload);
            total_msgs++;
            if(total_msgs % 1000 == 0) {
                System.out.println("++ sent " + total_msgs);
            }
            if(total_msgs % log_interval == 0) {
                //if(gnuplot_output == false)
                //  log.info(dumpStats(total_msgs));
            }
        }

    }


    void fetchResults() throws Exception {
        System.out.println("-- sent all messages. Asking receivers if they received all messages\n");

        int expected_responses=this.members.size();

        // now send DONE message (periodically re-send to make up for message loss over unreliable transport)
        // when all results have been received, dump stats and exit
        Data d2=new Data(Data.DONE);
        byte[] tmp=Util.objectToByteBuffer(d2);
        System.out.println("-- fetching results (from " + expected_responses + " members)");
        synchronized(this.results) {
            while((results.size()) < expected_responses) {
                transport.send(null, tmp);
                this.results.wait(1000);
            }
        }
        System.out.println("-- received all responses");
    }


    void dumpResults() {
        Object      member;
        Map.Entry   entry;
        HashMap     map;
        StringBuffer sb=new StringBuffer();
        sb.append("\n-- results:\n\n");

        for(Iterator it=results.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            member=entry.getKey();
            map=(HashMap)entry.getValue();
            sb.append("-- results from ").append(member).append(":\n");
            dump(map, sb);
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }


    void dumpSenders() {
        StringBuffer sb=new StringBuffer();
        dump(this.senders, sb);
        System.out.println(sb.toString());
    }

    void dump(HashMap map, StringBuffer sb) {
        Map.Entry  entry;
        Object     sender;
        MemberInfo mi;
        MemberInfo combined=new MemberInfo(0);
        combined.start=System.currentTimeMillis();
        combined.stop=System.currentTimeMillis();

        for(Iterator it2=map.entrySet().iterator(); it2.hasNext();) {
            entry=(Map.Entry)it2.next();
            sender=entry.getKey();
            mi=(MemberInfo)entry.getValue();
            combined.start=Math.min(combined.start, mi.start);
            combined.stop=Math.max(combined.stop, mi.stop);
            combined.num_msgs_expected+=mi.num_msgs_expected;
            combined.num_msgs_received+=mi.num_msgs_received;
            combined.total_bytes_received+=mi.total_bytes_received;
            sb.append("sender: ").append(sender).append(": ").append(mi).append("\n");
        }
        sb.append("\ncombined: ").append(combined).append("\n");
    }


    void runDiscoveryPhase() throws Exception {
        Data d=new Data(Data.DISCOVERY_REQ);
        transport.send(null, Util.objectToByteBuffer(d));
        sendDiscoveryResponse();

        synchronized(this.members) {
            System.out.println("-- waiting for " + num_members + " members to join");
            while(this.members.size() < num_members) {
                this.members.wait();
                System.out.println("-- members: " + this.members.size());
            }
        }
    }

    void sendDiscoveryResponse() throws Exception {
        Data d2=new Data(Data.DISCOVERY_RSP);
        if(sender) {
            d2.sender=true;
            d2.num_msgs=Long.parseLong(config.getProperty("num_msgs"));
        }
        transport.send(null, Util.objectToByteBuffer(d2));
    }


    public static void main(String[] args) {
        Properties config=new Properties();
        boolean sender=false;
        Test t=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-sender")) {
                config.put("sender", "true");
                sender=true;
                continue;
            }
            if(args[i].equals("-receiver")) {
                config.put("sender", "false");
                sender=false;
                continue;
            }
            if(args[i].equals("-config")) {
                String config_file=args[++i];
                config.put("config", config_file);
                continue;
            }
            if(args[i].equals("-props")) {
                String props=args[++i];
                config.put("props", props);
                continue;
            }
            help();
            return;
        }

        try {
            t=new Test();
            t.start(config);
            t.runDiscoveryPhase();
            if(sender) {
                t.sendMessages();
                t.fetchResults();
                t.dumpResults();
            }
            else {
                synchronized(t) {
                    t.wait(60000);
                }
                Util.sleep(2000);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if(t != null)
                t.stop();
        }
    }


    static void help() {
        System.out.println("Test [-help] ([-sender] | [-receiver]) [-config <config file>] [-props <stack config>]");
    }


}
