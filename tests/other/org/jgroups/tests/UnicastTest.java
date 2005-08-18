// $Id: UnicastTest.java,v 1.8 2005/08/18 09:45:25 belaban Exp $

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Vector;


/**
 * Tests the UNICAST by sending unicast messages between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTest implements Runnable {
    UnicastTest test;
    JChannel channel;
    final String groupname="UnicastTest-Group";
    Thread t=null;
    long sleep_time=0;
    boolean exit_on_end=false, busy_sleep=false;


    public static class Data implements Externalizable {
        public Data() {
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }

    public static class StartData extends Data {
        long num_values=0;

        public StartData() {
            super();
        }

        StartData(long num_values) {
            this.num_values=num_values;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(num_values);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            num_values=in.readLong();
        }
    }

    public static class Value extends Data {
        long value=0;

        public Value() {
            super();
        }

        Value(long value) {
            this.value=value;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(value);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value=in.readLong();
        }
    }


    public void init(String props, long sleep_time, boolean exit_on_end, boolean busy_sleep) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        channel=new JChannel(props);
        channel.connect(groupname);
        t=new Thread(this, "UnicastTest - receiver thread");
        t.start();
    }


    public void run() {
        Data data;
        Message msg;
        Object obj;
        boolean started=false;
        long start=0, stop=0;
        long current_value=0, tmp=0, num_values=0;
        long total_msgs=0, total_time=0, msgs_per_sec;

        while(true) {
            try {
                obj=channel.receive(0);
                if(obj instanceof View)
                    System.out.println("** view: " + obj);
                else
                    if(obj instanceof Message) {
                        msg=(Message)obj;
                        data=(Data)msg.getObject();

                        if(data instanceof StartData) {
                            if(started) {
                                System.err.println("UnicastTest.run(): received START data, but am already processing data");
                            }
                            else {
                                started=true;
                                current_value=0; // first value to be received
                                tmp=0;
                                num_values=((StartData)data).num_values;
                                start=System.currentTimeMillis();
                            }
                        }
                        else
                            if(data instanceof Value) {
                                tmp=((Value)data).value;
                                if(current_value + 1 != tmp) {
                                    System.err.println("-- message received (" + tmp + ") is not 1 greater than " + current_value);
                                }
                                else {
                                    current_value++;
                                    if(current_value % 1000 == 0)
                                        System.out.println("received " + current_value);
                                    if(current_value >= num_values) {
                                        stop=System.currentTimeMillis();
                                        total_time=stop - start;
                                        msgs_per_sec=(long)(num_values / (total_time / 1000.0));
                                        System.out.println("-- received " + num_values + " messages in " + total_time +
                                                           " ms (" + msgs_per_sec + " messages/sec)");
                                        started=false;
                                        if(exit_on_end)
                                            System.exit(0);
                                    }
                                }
                            }
                    }
            }
            catch(ChannelNotConnectedException not_connected) {
                System.err.println(not_connected);
                break;
            }
            catch(ChannelClosedException closed_ex) {
                System.err.println(closed_ex);
                break;
            }
            catch(TimeoutException timeout) {
                System.err.println(timeout);
                break;
            }
            catch(Throwable t) {
                System.err.println(t);
                started=false;
                current_value=0;
                tmp=0;
                Util.sleep(1000);
            }
        }
        // System.out.println("UnicastTest.run(): receiver thread terminated");
    }


    public void eventLoop() throws Exception {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [q] Quit ");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                sendMessages();
                break;
            case '2':
                printView();
                break;
            case '3':
                break;
            case '4':
                break;
            case '5':
                break;
            case '6':
                break;
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }


    void sendMessages() throws Exception {
        long num_msgs=getNumberOfMessages();
        Address receiver=getReceiver();
        Message msg;
        Value val=new Value(1);

        if(receiver == null) {
            System.err.println("UnicastTest.sendMessages(): receiver is null, cannot send messages");
            return;
        }

        System.out.println("sending " + num_msgs + " messages to " + receiver);
        msg=new Message(receiver, null, new StartData(num_msgs));
        channel.send(msg);

        for(int i=1; i <= num_msgs; i++) {
            val=new Value(i);
            msg=new Message(receiver, null, val);
            if(i % 1000 == 0)
                System.out.println("-- sent " + i);
            channel.send(msg);
            if(sleep_time > 0)
                Util.sleep(sleep_time, busy_sleep);
        }
        System.out.println("done sending " + num_msgs + " to " + receiver);
    }

    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }


    long getNumberOfMessages() {
        BufferedReader reader=null;
        String tmp=null;

        try {
            System.out.print("Number of messages to send: ");
            System.out.flush();
            System.in.skip(System.in.available());
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            return Long.parseLong(tmp);
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getNumberOfMessages(): " + e);
            return 0;
        }
    }

    Address getReceiver() {
        Vector mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(channel.getLocalAddress()))
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i) + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i));
            }
            System.out.flush();
            System.in.skip(System.in.available());
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            index=Integer.parseInt(tmp);
            return (Address)mbrs.elementAt(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }


    public static void main(String[] args) {
        long sleep_time=0;
        boolean exit_on_end=false;
        boolean busy_sleep=false;

        String udp_props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
                "ucast_recv_buf_size=32000;ucast_send_buf_size=64000;" +
                "mcast_send_buf_size=32000;mcast_recv_buf_size=64000;loopback=true):";

        String regular_props="PING(timeout=1000;num_initial_members=2):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800;max_xmit_size=8192):" +
                "UNICAST(timeout=2000,4000,6000;window_size=100;min_threshold=10;use_gms=false):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true)";

        String props=udp_props + regular_props;
        String loopback_props="LOOPBACK:" + regular_props;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-loopback".equals(args[i])) {
                props=loopback_props;
                continue;
            }
            if("-exit_on_end".equals(args[i])) {
                exit_on_end=true;
                continue;
            }
            if("-busy_sleep".equals(args[i])) {
                busy_sleep=true;
                continue;
            }
        }


        try {
            UnicastTest test=new UnicastTest();
            test.init(props, sleep_time, exit_on_end, busy_sleep);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-props <props>] [-sleep <time in ms between msg sends] " +
                           "[-loopback] [-exit_on_end] [-busy-sleep]");
    }
}
