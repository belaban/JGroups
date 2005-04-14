package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.PERF_TP;
import org.jgroups.stack.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Vector;

/**
 * Sends num_msgs up the stack. Stack has to have DUMMY_TP transport and PERF_TP top protocol
 * @author Bela Ban Feb 24, 2004
 * @version $Id: SimplePerfTest.java,v 1.1 2005/04/14 16:30:13 belaban Exp $
 */
public class SimplePerfTest {
    JChannel ch=null;
    PERF_TP  tp=null;
    DataInputStream in=null;
    DataOutputStream out=null;


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
            new SimplePerfTest().start(props, num_msgs, size);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void help() {
        System.out.println("PerfTpTest [-help] [-props <properties>] [-num <num msgs>] " +
                           "[-size <msg size (in bytes)]");
    }

    void start(String props, int num_msgs, int size) throws Exception {
        Message msg;
        Protocol transport;
        byte[] buf=new byte[size];

        ch=new JChannel(props);
        ch.connect("demo");
        tp=PERF_TP.getInstance();

        Vector protocols=ch.getProtocolStack().getProtocols();
        transport=(Protocol)protocols.lastElement();


        System.out.println("sending " + num_msgs + " up the stack");

        tp.setExpectedMessages(num_msgs); // this starts the time
        for(int i=0; i < num_msgs; i++) {
            msg=new Message(null, null, buf);
            transport.up(new Event(Event.MSG, msg));
            if(i % 10000 == 0) {
                System.out.println("passed up " + i + " messages");
            }
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
        double msgs_per_ms=num_msgs / (double)total;
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
