package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;




/**
 * Tests the time to multicast a message to everyone and then receive the responses
 * @author Bela Ban
 * @version $Id: SynchronousMessageSpeedTest.java,v 1.1 2005/07/22 10:30:21 belaban Exp $
 */
public class SynchronousMessageSpeedTest {
    Channel             channel;
    String              props=null;
    boolean             server=false; // role is client by default
    int                 num=1000, received=0;
    static final long   TIMEOUT=10000;



    public SynchronousMessageSpeedTest(String props, boolean server, int num) {
        this.props=props;
        this.server=server;
        this.num=num;
    }



    public void start() throws Exception {
        Object obj;
        Message msg;
        channel=new JChannel(props);
        channel.setOpt(Channel.LOCAL, Boolean.FALSE); // do *not* receive my own messages
        channel.connect("MessageDispatcherSpeedTestGroup");

        try {
            while(channel.getNumMessages() > 0)
                channel.receive(10); // clear the input queue

            if(server) {
                System.out.println("-- Started as server. Press ctrl-c to kill");
                int i=0;
                while(true) {
                    obj=channel.receive(0);
                    if(obj instanceof Message) {
                        msg=(Message)obj;
                        Message rsp=new Message(msg.getSrc(), null, null);
                        if(++received % 1000 == 0)
                            System.out.println("-- received " + received);
                        channel.send(rsp);
                    }
                }
            }
            else {
                sendMessages(num);
            }
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
        }
        finally {
            channel.close();
        }
    }


    void sendMessages(int num) throws Exception {
        long    start, stop;
        int     show=num/10;
        Object  obj;

        if(show <=0) show=1;
        start=System.currentTimeMillis();

        System.out.println("-- sending " + num + " messages");
        for(int i=1; i <= num; i++) {
            channel.send(new Message());
            if(i % show == 0)
                System.out.println("-- sent " + i);

            while(true) {
                obj=channel.receive(0);
                if(obj instanceof Message) {
                    received++;
                    if(received % show == 0)
                        System.out.println("-- received response: " + received);
                    break;
                }
            }
        }
        stop=System.currentTimeMillis();
        printStats(stop-start, num);
    }



    void printStats(long total_time, int num) {
        double throughput=((double)num)/((double)total_time/1000.0);
        System.out.println("time for " + num + " remote calls was " +
                           total_time + ", avg=" + (total_time / (double)num) +
                           "ms/invocation, " + (long)throughput + " calls/sec");
    }




    public static void main(String[] args) {
        String                 props=null;
        boolean                server=false;
        int                    num=1000;
        SynchronousMessageSpeedTest test;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-server".equals(args[i])) {
                server=true;
                continue;
            }
            if("-num".equals(args[i])) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }


        try {
            test=new SynchronousMessageSpeedTest(props, server, num);
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] [-server] [-num <number of calls>]");
    }


}
