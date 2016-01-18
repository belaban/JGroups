
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.AbstractRetransmitter;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.IOException;


/**
 * Adds a large number of messages (with gaps to simulate message loss) into NakReceiverWindow. Receives messages
 * on another thread. When the receiver thread has received all messages it prints out the time taken and terminates.
 *
 * @author Bela Ban
 */
public class NakReceiverWindowStressTest implements AbstractRetransmitter.RetransmitCommand {
    NakReceiverWindow win=null;
    final Address sender=Util.createRandomAddress("A");
    int num_msgs=1000, prev_value=0;
    double discard_prob=0.0; // discard 0% of all insertions
    long start, stop;
    boolean trace=false;
    boolean debug=false;


    public NakReceiverWindowStressTest(int num_msgs, double discard_prob, boolean trace) {
        this.num_msgs=num_msgs;
        this.discard_prob=discard_prob;
        this.trace=trace;
    }


    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        for(long i=first_seqno; i <= last_seqno; i++) {
            if(debug)
                out("-- xmit: " + i);
            Message m=new Message(null, sender, new Long(i));
            win.add(i, m);
        }
    }


    public void start() throws IOException, InterruptedException {
        System.out.println("num_msgs=" + num_msgs + "\ndiscard_prob=" + discard_prob);

        TimeScheduler timer=new DefaultTimeScheduler();
        try {
            win=new NakReceiverWindow(sender, this, 0, timer);
            start=System.currentTimeMillis();
            sendMessages(num_msgs);
        }
        finally {
            timer.stop();
        }
    }


    void sendMessages(int num_msgs) {
        Message msg;

        for(long i=1; i <= num_msgs; i++) {
            if(discard_prob > 0 && Util.tossWeightedCoin(discard_prob) && i <= num_msgs) {
                if(debug) out("-- discarding " + i);
            }
            else {
                if(debug) out("-- adding " + i);
                win.add(i, new Message(null, null, new Long(i)));
                if(trace && i % 1000 == 0)
                    System.out.println("-- added " + i);
                while((msg=win.remove()) != null)
                    processMessage(msg);
            }
        }
        while(true) {
            while((msg=win.remove()) != null)
                processMessage(msg);
        }
    }


    void processMessage(Message msg) {
        long i;

        i=((Long)msg.getObject()).longValue();
        if(prev_value + 1 != i) {
            System.err.println("** processMessage(): removed seqno (" + i + ") is not 1 greater than " +
                    "previous value (" + prev_value + ')');
            System.exit(0);
        }
        prev_value++;
        if(trace && i % 1000 == 0)
            System.out.println("Removed " + i);
        if(i == num_msgs) {
            stop=System.currentTimeMillis();
            long total=stop-start;
            double msgs_per_sec=num_msgs / (total/1000.0);
            double msgs_per_ms=num_msgs / (double)total;
            System.out.println("Inserting and removing " + num_msgs +
                    " messages into NakReceiverWindow took " + total + "ms");
            System.out.println("Msgs/sec: " + msgs_per_sec + ", msgs/ms: " + msgs_per_ms);
            System.out.println("<enter> to terminate");
            try {
                System.in.read();
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
            System.exit(0);
        }
    }


    static void out(String msg) {
        System.out.println(msg);
    }


    public static void main(String[] args) {
        NakReceiverWindowStressTest test;
        int num_msgs=1000;
        double discard_prob=0.0;
        boolean trace=false;


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-num_msgs".equals(args[i])) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if("-discard".equals(args[i])) {
                discard_prob=Double.parseDouble(args[++i]);
                continue;
            }
            if("-trace".equals(args[i])) {
                trace=true;
            }
        }


        test=new NakReceiverWindowStressTest(num_msgs, discard_prob, trace);
        try {
            test.start();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
    }


    static void help() {
        System.out.println("NakReceiverWindowStressTest [-help] [-num_msgs <number>] [-discard <probability>] " +
                "[-trace]");
    }

}
