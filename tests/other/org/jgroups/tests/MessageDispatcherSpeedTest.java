package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.util.Util;




/**
 * @author Bela Ban
 */
public class MessageDispatcherSpeedTest implements MembershipListener, RequestHandler {
    AbstractChannel channel;
    MessageDispatcher   disp;
    String              props=null;
    boolean             server=false; // role is client by default
    int                 num=1000, received=0;
    static final long   TIMEOUT=10000;



    public MessageDispatcherSpeedTest(String props, boolean server, int num) {
        this.props=props;
        this.server=server;
        this.num=num;
    }


    public Object handle(Message msg) throws Exception {
        received++;
        if(received % 1000 == 0)
            System.out.println("-- received " + received);
        return null;
    }

    public void start() throws Exception {
        channel=new JChannel(props);
        disp=new MessageDispatcher(channel, null, this, this);
        channel.connect("MessageDispatcherSpeedTestGroup");

        try {
            if(server) {
                System.out.println("-- Started as server. Press ctrl-c to kill");
                while(true) {
                    Util.sleep(10000);
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
            disp.stop();
        }
    }


    void sendMessages(int num) throws Exception {
        long    start, stop;
        int     show=num/10;

        if(show <=0) show=1;
        start=System.currentTimeMillis();
        RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, TIMEOUT).setFlags(Message.Flag.DONT_BUNDLE, Message.NO_FC);

        System.out.println("-- sending " + num + " messages");
        for(int i=1; i <= num; i++) {
            disp.castMessage(null, new Message(), opts);
            if(i % show == 0)
                System.out.println("-- sent " + i);
        }
        stop=System.currentTimeMillis();
        printStats(stop-start, num);
    }



    static void printStats(long total_time, int num) {
        double throughput=((double)num)/((double)total_time/1000.0);
        System.out.println("time for " + num + " remote calls was " +
                           total_time + ", avg=" + (total_time / (double)num) +
                           "ms/invocation, " + (long)throughput + " calls/sec");
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }



    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }

    public static void main(String[] args) {
        String                 props=null;
        boolean                server=false;
        int                    num=1000;
        MessageDispatcherSpeedTest test;

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
            test=new MessageDispatcherSpeedTest(props, server, num);
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("MessageDispatcherSpeedTest [-help] [-props <props>] [-server] [-num <number of calls>]");
    }


}
