package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.PERF_TP;
import org.jgroups.stack.Protocol;

import java.io.*;
import java.util.Vector;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;

/**
 * Test of PERF_TP. Requirement: transport needs to be PERF_TP
 * @author Bela Ban Feb 24, 2004
 * @version $Id: PerfTpTest.java,v 1.10 2009/04/09 09:11:20 belaban Exp $
 */
public class PerfTpTest {
    JChannel ch=null;
    PERF_TP  tp=null;
    DataInputStream in=null;
    DataOutputStream out=null;


    public static void main(String[] args) {
        String props=null;
        int    num_msgs=1000;
        int    size=1000; // bytes
        String   file_name=null;
        boolean  write=true;

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
            if("-file_name".equals(args[i])) {
                file_name=args[++i];
                continue;
            }
            if("-write".equals(args[i])) {
                write=true;
                continue;
            }
            if("-read".equals(args[i])) {
                write=false;
                continue;
            }
            help();
            return;
        }

        try {
            new PerfTpTest().start(props, num_msgs, size, file_name, write);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void help() {
        System.out.println("PerfTpTest [-help] [-props <properties>] [-num <num msgs>] " +
                           "[-size <msg size (in bytes)] [-file_name <filename>] [-write] [-read]");
    }

    void start(String props, int num_msgs, int size, String file_name, boolean write) throws Exception {
        Message msg;
        Protocol transport;
        byte[] buf=new byte[size];
        Address local_addr;
        View    view;

        if(file_name != null) {
            if(write)
                out=new DataOutputStream(new FileOutputStream(file_name));
            else
                in=new DataInputStream(new FileInputStream(file_name));
        }

        ch=new JChannel(props);
        ch.setReceiver(new ExtendedReceiverAdapter());
        ch.connect("demo");
        tp=PERF_TP.getInstance();
        local_addr=ch.getAddress();
        Vector members=new Vector();
        members.add(local_addr);
        view=new View(local_addr, 0, members);
        ch.down(new Event(Event.BECOME_SERVER));
        ch.down(new Event(Event.VIEW_CHANGE, view));

        if(write) {
            tp.setExpectedMessages(num_msgs);
            for(int i=0; i < num_msgs; i++) {
                msg=new Message(null, local_addr, buf);
                ch.send(msg);
                if(out != null)
                    msg.writeTo(out);
                if(i % 1000 == 0)
                    System.out.println("sent " + i + " messages");
            }
        }
        else {
            List msgs=new LinkedList();
            Vector protocols=ch.getProtocolStack().getProtocols();
            transport=(Protocol)protocols.lastElement();
            int i=0;
            while(true) {
                msg=new Message();
                try {
                    msg.readFrom(in);
                    msgs.add(msg);
                }
                catch(EOFException eof) {
                    break;
                }
            }

            num_msgs=msgs.size();
            System.out.println("read " + num_msgs + " msgs from file");
            tp.setExpectedMessages(msgs.size()); // this starts the time
            for(Iterator it=msgs.iterator(); it.hasNext();) {
                msg=(Message)it.next();
                i++;
                transport.up(new Event(Event.MSG, msg));
                if(i % 10000 == 0) {
                    System.out.println("passed up " + i + " messages");
                }
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
