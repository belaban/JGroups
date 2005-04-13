package org.jgroups.tests;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.PERF_TP;

/**
 * Test of PERF_TP. Requirement: transport needs to be PERF_TP
 * @author Bela Ban Feb 24, 2004
 * @version $Id: PerfTpTest.java,v 1.5 2005/04/13 13:04:11 belaban Exp $
 */
public class PerfTpTest {
    JChannel ch=null;
    PERF_TP  tp=null;

    public static void main(String[] args) {
        String props=null;
        int    num_msgs=1000;
        int    size=1000; // bytes

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])){
                props=args[++i];
                continue;
            }
            if("-num".equals(args[i])) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if("-size".equals(args[i])) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        try {
            new PerfTpTest().start(props, num_msgs, size);
        }
        catch(ChannelException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void help() {
        System.out.println("PerfTpTest [-help] [-props <properties>] [-num <num msgs>] [-size <msg size (in bytes)]");
    }

    void start(String props, int num_msgs, int size) throws ChannelException, InterruptedException {
        Message msg;
        byte[] buf=new byte[size];
        ch=new JChannel(props);
        ch.connect("demo");
        tp=PERF_TP.getInstance();
        tp.setExpectedMessages(num_msgs);
        for(int i=0; i < num_msgs; i++) {
            msg=new Message(null, null, buf);
            ch.send(msg);
            if(i % 1000 == 0)
                System.out.println("sent " + i + " messages");
        }
        synchronized(tp) {
            if(tp.done()) {
                ;
            }
            else {
                tp.wait();
            }
        }
        long total=tp.getTotalTime();
        double msgs_per_ms=num_msgs / total;
        double msgs_per_sec=msgs_per_ms * 1000;
        double time_per_msg=total / (double)num_msgs;
        double usec_per_msg=time_per_msg * 1000;

        System.out.println("num_msgs = " + num_msgs + ", total_time = " + total + "ms");
        System.out.println("msgs/millisec = " + msgs_per_ms + ", msgs/sec = " + msgs_per_sec +
                "\ntime/msg = " + time_per_msg + " ms" +
                " (" + usec_per_msg + " usec/msg)");
        ch.close();
    }
}
