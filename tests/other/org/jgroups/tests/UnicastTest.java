
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Perf tests the UNICAST protocol by sending unicast messages between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTest {
    protected JChannel             channel;
    protected final MyReceiver     receiver=new MyReceiver();
    protected long                 sleep_time=0;
    protected boolean              oob=false, dont_bundle=false;
    protected int                  num_threads=1;
    protected int                  num_msgs=100000, msg_size=1000;

    protected static final byte    START = 1; // | num_msgs (long) |
    protected static final byte    DATA  = 2; // | length (int) | data (byte[]) |


    public void init(Protocol[] props, long sleep_time, String name) throws Exception {
        _init(new JChannel(props), sleep_time, name);
    }

    public void init(String props, long sleep_time, String name) throws Exception {
        _init(new JChannel(props), sleep_time, name);
    }

    protected void _init(JChannel ch, long sleep_time, String name) throws Exception {
        this.sleep_time=sleep_time;
        channel=ch;
        if(name != null)
            channel.setName(name);
        channel.connect(getClass().getSimpleName());
        channel.setReceiver(receiver);

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups-" + name, channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel with JMX failed: " + ex);
        }
    }


    public void eventLoop() throws Exception {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print conns [5] Trash all conns" +
                    "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    "\n[o] Toggle OOB (" + oob + ") [b] Toggle dont_bundle (" + dont_bundle + ")\n[q] Quit\n");
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
                printConnections();
                break;
            case '5':
                removeAllConnections();
                break;
            case '6':
                setSenderThreads();
                break;
            case '7':
                setNumMessages();
                break;
            case '8':
                setMessageSize();
                break;
            case 'o':
                oob=!oob;
                System.out.println("oob=" + oob);
                break;
                case 'b':
                    dont_bundle=!dont_bundle;
                    System.out.println("dont_bundle = " + dont_bundle);
                    break;
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }

    protected void printConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            System.out.println(((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println(((UNICAST2)prot).printConnections());
        else if(prot instanceof UNICAST3)
            System.out.println(((UNICAST3)prot).printConnections());
    }


    protected void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
        else if(prot instanceof UNICAST3)
            ((UNICAST3)prot).removeAllConnections();
    }


    void sendMessages() throws Exception {
        Address destination=getReceiver();
        if(destination == null) {
            System.err.println("UnicastTest.sendMessages(): receiver is null, cannot send messages");
            return;
        }

        if(num_threads > 1 && num_msgs % num_threads != 0) {
            System.err.println("num_msgs (" + num_msgs + " ) has to be divisible by num_threads (" + num_threads + ")");
            return;
        }

        System.out.println("sending " + num_msgs + " messages (" + Util.printBytes(msg_size) +
                ") to " + destination + ": oob=" + oob + ", " + num_threads + " sender thread(s)");
        ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE).put(START).putLong(num_msgs);
        Message msg=new Message(destination, buf.array());
        channel.send(msg);

        long print=num_msgs / 10;
        int msgs_per_sender=num_msgs / num_threads;
        Sender[] senders=new Sender[num_threads];
        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(msgs_per_sender, msg_size, destination, (int)print);
        for(Sender sender: senders)
            sender.start();
        for(Sender sender: senders)
            sender.join();
        System.out.println("done sending " + num_msgs + " to " + destination);
    }

    void setSenderThreads() throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        int old=this.num_threads;
        this.num_threads=threads;
        System.out.println("sender threads set to " + num_threads + " (from " + old + ")");
    }

    void setNumMessages() throws Exception {
        num_msgs=Util.readIntFromStdin("Number of messages: ");
        System.out.println("Set num_msgs=" + num_msgs);
    }

    void setMessageSize() throws Exception {
        msg_size=Util.readIntFromStdin("Message size: ");
        System.out.println("set msg_size=" + msg_size);
    }

    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }



    protected Address getReceiver() {
        try {
            List<Address> mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            int i=0;
            for(Address mbr: mbrs) {
                if(mbr.equals(channel.getAddress()))
                    System.out.println("[" + i + "]: " + mbr + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbr);
                i++;
            }
            System.out.flush();
            System.in.skip(System.in.available());
            BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
            String tmp=reader.readLine().trim();
            int index=Integer.parseInt(tmp);
            return mbrs.get(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }


    public static void main(String[] args) {
        long sleep_time=0;
        String props=null;
        String name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }


        try {
            UnicastTest test=new UnicastTest();
            test.init(props, sleep_time, name);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-props <props>] [-sleep <time in ms between msg sends] [-name name]");
    }


    protected class Sender extends Thread {
        protected final int     number_of_msgs;
        protected final Address destination;
        protected final int     print;
        protected final byte[]  buf;

        public Sender(int num_msgs, int msg_size, Address destination, int print) {
            this.number_of_msgs=num_msgs;
            this.destination=destination;
            this.print=print;
            buf=ByteBuffer.allocate(Global.INT_SIZE + msg_size).put(DATA).array();
        }

        public void run() {
            for(int i=1; i <= number_of_msgs; i++) {
                try {
                    Message msg=new Message(destination, buf);
                    if(oob)
                        msg.setFlag(Message.Flag.OOB);
                    if(dont_bundle)
                        msg.setFlag(Message.Flag.DONT_BUNDLE);
                    if(i > 0 && print > 0 && i % print == 0)
                        System.out.println("-- sent " + i);
                    channel.send(msg);
                    if(sleep_time > 0)
                        Util.sleep(sleep_time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    protected class MyReceiver extends ReceiverAdapter {
        protected long        start=0;
        protected long        print;
        protected AtomicLong  current_value=new AtomicLong(0), total_bytes=new AtomicLong(0);


        public void receive(Message msg) {
            byte[] buf=msg.getRawBuffer();
            byte   type=buf[msg.getOffset()];

            switch(type) {
                case START:
                    ByteBuffer tmp=ByteBuffer.wrap(buf, 1+msg.getOffset(), Global.LONG_SIZE);
                    num_msgs=(int)tmp.getLong();
                    print=num_msgs / 10;
                    current_value.set(0);
                    total_bytes.set(0);
                    start=System.currentTimeMillis();
                    break;
                case DATA:
                    long new_val=current_value.incrementAndGet();
                    total_bytes.addAndGet(msg.getLength() - Global.INT_SIZE);
                    if(print > 0 && new_val % print == 0)
                        System.out.println("received " + new_val);
                    if(new_val >= num_msgs) {
                        long time=System.currentTimeMillis() - start;
                        double msgs_sec=(current_value.get() / (time / 1000.0));
                        double throughput=total_bytes.get() / (time / 1000.0);
                        System.out.println(String.format("\nreceived %d messages in %d ms (%.2f msgs/sec), throughput=%s",
                                                         current_value.get(), time, msgs_sec, Util.printBytes(throughput)));
                        break;
                    }
                    break;
                default:
                    System.err.println("Type " + type + " is invalid");
            }
        }

        public void viewAccepted(View new_view) {
            System.out.println("** view: " + new_view);
        }
    }

}
