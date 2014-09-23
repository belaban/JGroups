
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;

import javax.management.MBeanServer;
import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Perf tests the UNICAST protocol by sending unicast messages between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTest extends ReceiverAdapter {
    private JChannel channel;
    private final MyReceiver receiver=new MyReceiver();
    static final String groupname="UnicastTest-Group";
    private long sleep_time=0;
    private boolean exit_on_end=false, busy_sleep=false, oob=false;
    private int num_threads=1;
    private int num_msgs=100000, msg_size=1000;




    public void init(String props, long sleep_time, boolean exit_on_end, boolean busy_sleep, String name) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        channel.connect(groupname);
        channel.setReceiver(receiver);

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }
    }


    public void eventLoop() throws Exception {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print conns [4] Trash conn [5] Trash all conns" +
                    "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    "\n[o] Toggle OOB (" + oob + ")\n[q] Quit\n");
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
            case '4':
                removeConnection();
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
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }

    private void printConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            System.out.println(((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println(((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
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
        byte[] buf=Util.objectToByteBuffer(new StartData(num_msgs));
        Message msg=new Message(destination, null, buf);
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



    private Address getReceiver() {
        List<Address> mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=channel.getView().getMembers();
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
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            index=Integer.parseInt(tmp);
            return mbrs.get(index); // index out of bounds caught below
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
            if("-exit_on_end".equals(args[i])) {
                exit_on_end=true;
                continue;
            }
            if("-busy_sleep".equals(args[i])) {
                busy_sleep=true;
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
            test.init(props, sleep_time, exit_on_end, busy_sleep, name);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-props <props>] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep] [-name name]");
    }


    public abstract static class Data implements Streamable {
    }

    public static class StartData extends Data {
        long num_values=0;

        public StartData() {}

        StartData(long num_values) {
            this.num_values=num_values;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(num_values);
        }

        public void readFrom(DataInput in) throws Exception {
            num_values=in.readLong();
        }
    }

    public static class Value extends Data {
        long value=0;
        byte[] buf=null;

        public Value() {
        }

        Value(long value, int len) {
            this.value=value;
            if(len > 0)
                this.buf=new byte[len];
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(value);
            if(buf != null) {
                out.writeInt(buf.length);
                out.write(buf, 0, buf.length);
            }
            else {
                out.writeInt(0);
            }
        }

        public void readFrom(DataInput in) throws Exception {
            value=in.readLong();
            int len=in.readInt();
            if(len > 0) {
                buf=new byte[len];
                in.readFully(buf, 0, len);
            }
        }
    }

    private class Sender extends Thread {
        private final int number_of_msgs;
        private final int message_size;
        private final Address destination;
        private final int print;

        public Sender(int num_msgs, int msg_size, Address destination, int print) {
            this.number_of_msgs=num_msgs;
            this.message_size=msg_size;
            this.destination=destination;
            this.print=print;
        }

        public void run() {
            for(int i=1; i <= number_of_msgs; i++) {
                Value val=new Value(i, message_size);
                byte[] buf=new byte[0];
                try {
                    buf=Util.objectToByteBuffer(val);
                    Message msg=new Message(destination, null, buf);
                    if(oob)
                        msg.setFlag(Message.Flag.OOB);
                    if(i > 0 && print > 0 && i % print == 0)
                        System.out.println("-- sent " + i);
                    channel.send(msg);
                    if(sleep_time > 0)
                        Util.sleep(sleep_time, busy_sleep);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class MyReceiver extends ReceiverAdapter {
        private boolean started=false;
        private long start=0, stop=0;
        private long tmp=0, num_values=0;
        private long total_time=0, msgs_per_sec, print;
        private AtomicLong current_value=new AtomicLong(0), total_bytes=new AtomicLong(0);


        public void receive(Message msg) {
            Data data;
            try {
                data=(Data)Util.objectFromByteBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            }
            catch(Exception e) {
                e.printStackTrace();
                return;
            }

            if(data instanceof StartData) {
                if(started) {
                    System.err.println("UnicastTest.run(): received START data, but am already processing data");
                }
                else {
                    started=true;
                    current_value.set(0); // first value to be received
                    tmp=0;
                    num_values=((StartData)data).num_values;
                    print=num_values / 10;
                    total_bytes.set(0);
                    start=System.currentTimeMillis();
                }
            }
            else
            if(data instanceof Value) {
                tmp=((Value)data).value;

                long new_val=current_value.incrementAndGet();
                if(((Value)data).buf != null)
                    total_bytes.addAndGet(((Value)data).buf.length);
                if(print > 0 && new_val % print == 0)
                    System.out.println("received " + current_value);
                if(new_val >= num_values) {
                    stop=System.currentTimeMillis();
                    total_time=stop - start;
                    msgs_per_sec=(long)(num_values / (total_time / 1000.0));
                    double throughput=total_bytes.get() / (total_time / 1000.0);
                    System.out.println("-- received " + num_values + " messages (" + Util.printBytes(total_bytes.get()) +
                            ") in " + total_time + " ms (" + msgs_per_sec + " messages/sec, " +
                            Util.printBytes(throughput) + " / sec)");
                    started=false;
                    if(exit_on_end)
                        System.exit(0);
                }
            }
        }

        public void viewAccepted(View new_view) {
            System.out.println("** view: " + new_view);
        }
    }

}
