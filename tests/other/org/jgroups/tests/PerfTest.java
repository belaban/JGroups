// $Id: PerfTest.java,v 1.5 2004/07/05 06:10:45 belaban Exp $

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Test which multicasts n messages to all members. Measures the time until all members have received
 * all messages from all senders. Start a number of members (e.g. 4). Wait until all of them are up and
 * have joined the group. Then press 's' for all senders to start multicasting messages. When you see all
 * *--* DONE messages for all senders, press 'a' to see the total stats.
 * @author Bela Ban
 */
public class PerfTest implements MessageListener, MembershipListener{

    /** HashMap<Address, Entry>. Stores received multicasts. Keyed by sender */
    HashMap data=new HashMap();

    /** Keeps track of membership */
    Vector mbrs=new Vector();

    /** Channel properties */
    String props=null;

    /** Sits on top of the channel */
    PullPushAdapter adapter=null;

    /** My channel for sending and receiving messages */
    JChannel ch=null;

    /** Am I a sender as well ? */
    boolean sender=true;

    /** Sleep time between bursts in milliseconds. 0 means no sleep */
    long sleep_time=10;

    /** Use busy sleeping ? (see #Util.sleep(long,boolean) for details) */
    boolean busy_sleep=false;

    /** Number of bursts. Total number of messages is <tt>num_bursts * num_msgs_per_burst</tt> */
    int num_bursts=100;

    /** Number of messages per burst. After a burst we sleep for <tt>sleep_time</tt> msecs */
    int num_msgs_per_burst=10;

    /** Size of a message in bytes */
    int msg_size=10000;

    /** The buffer to be sent (will be <tt>msg_size</tt> bytes) */
    byte[] buf=null;

    /** Number of messages sent by us */
    long sent_msgs=0;

    final static String HDRNAME="PerfHeaderName";




    public PerfTest(String props, int num_bursts, int num_msgs_per_burst,
                    int msg_size, long sleep_time, boolean sender) {
        this.props=props;
        this.num_bursts=num_bursts;
        this.num_msgs_per_burst=num_msgs_per_burst;
        this.msg_size=msg_size;
        this.sleep_time=sleep_time;
        this.buf=new byte[msg_size];
        this.sender=sender;
    }



    public void start() throws Exception {
        try {
            ch=new JChannel(props);
            ch.connect("PerfTest-Group");
            adapter=new PullPushAdapter(ch, this, this);
            mainLoop();
        }
        finally {
            if(ch != null)
                ch.close();
        }
    }

    void mainLoop() throws Exception {
        boolean looping=true;
        int     choice;
        while(looping) {
            choice=choice();
            switch(choice) {
                case 'q': case 'x':
                    looping=false;
                    break;
                case 's':
                    MyHeader hdr=new MyHeader(MyHeader.START, num_bursts * num_msgs_per_burst);
                    Message start_msg=new Message(null, null, null);
                    start_msg.putHeader(HDRNAME, hdr);
                    adapter.send(start_msg);
                    break;
                case 'c':
                    Message clear_msg=new Message();
                    clear_msg.putHeader(HDRNAME, new MyHeader(MyHeader.CLEAR, 0));
                    adapter.send(clear_msg);
                    break;
                case 't':
                    printStats();
                    break;
                case 'p':
                    printParams();
                    break;
                case 'v':
                    System.out.println("-- view: " + ch.getView());
                    break;
                case 'a':
                    printStatsForAllSenders();
                    break;
            }
        }
    }


    private void printStatsForAllSenders() {
        long  start_time=0, stop_time=0, total_time;
        Entry entry;
        int   num_msgs=0, num_senders=0;

        for(Iterator it=data.values().iterator(); it.hasNext();) {
            entry=(Entry)it.next();
            if(entry.num_received > 0) {
                num_msgs+=entry.num_received;
                num_senders++;

                // get the earliest start time
                if(start_time == 0)
                    start_time=entry.start;
                else {
                    start_time=Math.min(start_time, entry.start);
                }

                // get the latest stop time
                if(stop_time == 0) {
                    stop_time=entry.stop;
                }
                else {
                    stop_time=Math.max(stop_time, entry.stop);
                }
            }
        }

        total_time=stop_time - start_time;

        StringBuffer sb=new StringBuffer();
        sb.append("total number of messages sent by me: ").append(sent_msgs).append('\n');
        sb.append("total number of messages received: ").append(num_msgs).append('\n');
        sb.append("total number of senders: ").append(num_senders).append('\n');
        sb.append("total time: ").append(total_time).append(" ms\n");
        sb.append("msgs/sec: ").append((double)num_msgs / (total_time/1000.0)).append('\n');
        sb.append("throughput (kb/sec): ").append((num_msgs * msg_size/1000.0) / (total_time / 1000.0)).append('\n');
        System.out.println(sb.toString());
    }


    private void printParams() {
        System.out.println("num_bursts: " + num_bursts + '\n' +
                           "num_msgs_per_burst: " + num_msgs_per_burst + '\n' +
                           "msg_size: " + msg_size + '\n' +
                           "sleep_time: " + sleep_time + '\n' +
                           "sender: " + sender);
    }

    private void printStats() {
        for(Iterator it=data.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            System.out.println("stats for " + entry.getKey() + "");
            System.out.println(((Entry)entry.getValue()).printStats() + '\n');
        }
    }


    void sendMessages() {
        MyHeader hdr;
        Message  msg;
        int      seqno=0;
        long     start, stop;

        if(sender == false) {
            System.out.println("-- I'm not a sender; will not send messages");
            return;
        }
        else {
            System.out.println("-- sending " + num_bursts * num_msgs_per_burst + " msgs");
        }

        sent_msgs=0;

        try {
            start=System.currentTimeMillis();
            for(int i=0; i < num_bursts; i++) {
                for(int j=0; j < num_msgs_per_burst; j++) {
                    hdr=new MyHeader(MyHeader.DATA, seqno++);
                    msg=new Message(null, null, buf);
                    msg.putHeader(HDRNAME, hdr);
                    adapter.send(msg);
                    sent_msgs++;
                    if(sent_msgs % 100 == 0)
                        System.out.println("++ sent " + sent_msgs);
                }
                Util.sleep(sleep_time);
            }
            stop=System.currentTimeMillis();
            System.out.println("-- sent " + num_bursts * num_msgs_per_burst + " msgs (in " +
                               (stop-start) + " ms)");
//            System.out.flush();
//            Util.sleep(1000);
//            System.exit(1);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }



    int choice() throws Exception {
        System.out.println("s=send, c=clear, t=print stats, p=print parameters v=view, " +
                           "a=times for all messages, q=quit\nChoice: ");
        System.out.flush();
        System.in.skip(System.in.available());
        int c=System.in.read();
        System.out.flush();
        return c;
    }


    public void receive(Message msg) {
        Address  sender=msg.getSrc();
        MyHeader hdr=(MyHeader)msg.removeHeader(HDRNAME);
        if(hdr == null) {
            System.err.println("-- error: header was null");
            return;
        }
        switch(hdr.type) {
            case MyHeader.START:
                updateTimestamp();

                new Thread() {
                    public void run() {
                        // needs to be done in a separate thread; otherwise we cannot receive
                        // data messages until we have sent all messages (sendMessages() returned).
                        sendMessages();
                    }
                }.start();


                break;
            case MyHeader.DATA:
                Entry entry=(Entry)data.get(sender);
                if(entry == null) {
                    System.err.println("-- received a message from " + sender + ", who is not in the list");
                }
                else {
                    entry.add(hdr.seqno);
                    if((hdr.seqno) % 100 == 0)
                        System.out.println("-- received " + sender + ':' + hdr.seqno);
                    if(entry.getNumReceived() >= num_bursts * num_msgs_per_burst) {
                        if(entry.done())
                            System.out.println("*--* " + sender + " DONE");
                    }
                }
                break;
            case MyHeader.DONE:

                break;
            case MyHeader.CLEAR:
                clear();
                break;
            default:
                break;
        }
    }

    private void updateTimestamp() {
        for(Iterator it=data.values().iterator(); it.hasNext();) {
            Entry entry=(Entry)it.next();
            entry.start=System.currentTimeMillis();
        }
    }

    void clear() {
        System.out.println("-- clearing the data");
        data.clear();
        for(int i=0; i < mbrs.size(); i++)
            data.put(mbrs.elementAt(i), new Entry(num_bursts * num_msgs_per_burst));
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
        ;
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view.getMembers());
        mbrs.clear();
        mbrs.addAll(new_view.getMembers());
        clear();
    }

    public void suspect(Address suspected_mbr) {
        ;
    }

    public void block() {
        ;
    }



    public static void main(String[] args) {
        String   props=null;
        int      num_bursts=100;
        int      num_msgs_per_burst=10;
        long     sleep_time=10;
        int      msg_size=10000; // in bytes
        boolean  sender=true;

        PerfTest t;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-num_bursts".equals(args[i])) {
                num_bursts=Integer.parseInt(args[++i]);
                continue;
            }
            if("-num_msgs_per_burst".equals(args[i])) {
                num_msgs_per_burst=Integer.parseInt(args[++i]);
                continue;
            }
            if("-sleep_time".equals(args[i])) {
                sleep_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-msg_size".equals(args[i])) {
                msg_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-sender".equals(args[i])) {
                sender=Boolean.valueOf(args[++i]).booleanValue();
                continue;
            }
            help();
            return;
        }
        try {
            t=new PerfTest(props, num_bursts, num_msgs_per_burst, msg_size, sleep_time, sender);
            t.start();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
        }
    }

    static void help() {
        System.out.println("PerfTest [-help] [-props <properties>] [-num_bursts <num>] " +
                           "[-num_msgs_per_burst <num>] [-sleep_time <number of msecs>] " +
                           "[-msg_size <bytes>] [-sender <true/false>]");
    }




    public static class MyHeader extends Header {
        public static final int DATA  = 1;
        public static final int START = 2;
        public static final int CLEAR = 3;
        public static final int DONE  = 4;

        int      type=0;
        int      seqno=-1;


        public MyHeader() {

        }

        public MyHeader(int type, int seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        public long size() {
            return 16;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
            switch(type) {
                case DATA: sb.append("DATA (seqno=").append(seqno).append(')'); break;
                case START: sb.append("START"); break;
                case CLEAR: sb.append("CLEAR"); break;
                default: sb.append("<n/a>"); break;
            }
            return sb.toString();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            out.writeInt(seqno);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            seqno=in.readInt();
        }

    }



    class Entry {
        long   start=0, stop=0;
        int    num_received=0;
        int[]  seqnos=null;


        Entry(int num) {
            seqnos=new int[num];
            for(int i=0; i < seqnos.length; i++)
                seqnos[i]=-1;
            start=System.currentTimeMillis();
        }

        void add(int seqno) {
            if(seqnos != null)
                seqnos[seqno]=seqno;
            num_received++;
            if(num_received >= seqnos.length) {
                if(done())
                    stop=System.currentTimeMillis();
            }
        }

        boolean done() {
            if(seqnos == null)
                return false;
            for(int i=0; i < seqnos.length; i++)
                if(seqnos[i] < 0)
                    return false;
            return true;
        }

        int getNumReceived() {
            return num_received;
        }

        int getRealReceived() {
            int num=0;
            if(seqnos == null) return 0;
            for(int i=0; i < seqnos.length; i++) {
                if(seqnos[i] > -1)
                    num++;
            }
            return num;
        }

        String printStats() {
            StringBuffer sb=new StringBuffer();
            sb.append("done=").append(done()).append('\n');
            sb.append("number of messages received: ").append(getRealReceived()).append('\n');
            sb.append("total time: ").append(stop-start).append(" ms\n");
            sb.append("msgs/sec: ").append((double)getRealReceived() / ((stop-start)/1000.0)).append('\n');
            sb.append("throughput (kb/sec): ").append((getRealReceived() * msg_size/1000.0) / ((stop-start) / 1000.0)).append('\n');
            return sb.toString();
        }
    }


}
