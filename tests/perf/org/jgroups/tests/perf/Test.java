package org.jgroups.tests.perf;

import org.apache.log4j.Logger;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.FileReader;
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



    public void start(Properties config) throws Exception {
        String          config_file="config.txt";
        BufferedReader  fileReader;
        String          line;
        String          key, val;
        StringTokenizer st;

        this.config=config;
        Trace.init();

        sender=new Boolean(config.getProperty("sender")).booleanValue();
        config_file=config.getProperty("config");

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
            config.put(key, val);
        }
        fileReader.close();
        StringBuffer sb=new StringBuffer();
        for(Iterator it=config.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(":\t").append(entry.getValue()).append("\n");
        }
        sb.append("\n");
        System.out.println("Configuration is: " + sb.toString());
        Logger.getLogger(Test.class).info("main(): " + sb.toString());

        props=config.getProperty("props");
        num_members=Integer.parseInt(config.getProperty("num_members"));

        String transport_name=config.getProperty("transport");
        transport=(Transport)Thread.currentThread().getContextClassLoader().loadClass(transport_name).newInstance();
        transport.create(config);
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
                            System.out.println("-- received " + info.num_msgs_received + " messages");
                        if(info.num_msgs_received >= info.num_msgs_expected) {
                            info.done=true;
                            if(info.stop == 0)
                                info.stop=System.currentTimeMillis();
                            if(allReceived()) {
                                all_received=true;
                                if(stop == 0)
                                    stop=System.currentTimeMillis();
                                sendResults();
                            }
                        }
                    }
                    else {
                        System.err.println("-- sender " + sender + " not found in senders hashmap");
                    }
                    break;

                case Data.DONE:
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
                        }
                    }
                    else {
                        System.err.println("-- sender " + sender + " not found in senders hashmap");
                    }
                    break;

                case Data.RESULTS:
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
        boolean gnuplot_output=Boolean.getBoolean(config.getProperty("gnuplot_output", "false"));
        byte[] buf=new byte[msg_size];
        for(int i=0; i < msg_size; i++)
            buf[i]='.';
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
        System.out.println("Sent all messages. Asking receivers if they received all messages\n");


        // now send DONE message (periodically re-send to make up for message loss over unreliable transport)
        // when all results have been received, dump stats and exit
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

        config.put("sender", "false");
        config.put("num_msgs", "1000");
        config.put("msg_size", "1000");
        config.put("num_members", "2");
        config.put("num_senders", "1");
        config.put("log_interval", "1000");
        config.put("transport", "org.jgroups.tests.perf.transports.JGroupsTransport");

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
            if(sender)
                t.sendMessages();

            Thread.sleep(15000);
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
