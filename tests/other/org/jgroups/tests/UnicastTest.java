// $Id: UnicastTest.java,v 1.16 2010/01/08 14:32:55 belaban Exp $

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;

import javax.management.MBeanServer;
import java.io.*;
import java.util.Vector;


/**
 * Tests the UNICAST by sending unicast messages between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTest extends ReceiverAdapter {
    private JChannel channel;
    private final MyReceiver receiver=new MyReceiver();
    static final String groupname="UnicastTest-Group";
    private long sleep_time=0;
    private boolean exit_on_end=false, busy_sleep=false;




    public void init(String props, long sleep_time, boolean exit_on_end, boolean busy_sleep) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        channel=new JChannel(props);
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
            System.out.print("[1] Send msgs [2] Print view [3] Print conns [4] Trash conn [5] Trash all conns [q] Quit ");
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
        UNICAST unicast=(UNICAST)channel.getProtocolStack().findProtocol(UNICAST.class);
        System.out.println("connections:\n" + unicast.printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            UNICAST unicast=(UNICAST)channel.getProtocolStack().findProtocol(UNICAST.class);
            unicast.removeConnection(member);
        }
    }

    private void removeAllConnections() {
        UNICAST unicast=(UNICAST)channel.getProtocolStack().findProtocol(UNICAST.class);
        unicast.removeAllConnections();
    }


    void sendMessages() throws Exception {
        long num_msgs=Util.readLongFromStdin("Number of messages: ");
        int msg_size=Util.readIntFromStdin("Message size: ");
        Address destination=getReceiver();
        Message msg;

        if(destination == null) {
            System.err.println("UnicastTest.sendMessages(): receiver is null, cannot send messages");
            return;
        }

        System.out.println("sending " + num_msgs + " messages to " + destination);
        byte[] buf=Util.objectToByteBuffer(new StartData(num_msgs));
        msg=new Message(destination, null, buf);

        channel.send(msg);
        int print=(int)(num_msgs / 10);

        for(int i=1; i <= num_msgs; i++) {
            Value val=new Value(i, msg_size);
            buf=Util.objectToByteBuffer(val);
            msg=new Message(destination, null, buf);
            if(i % print == 0)
                System.out.println("-- sent " + i);
            channel.send(msg);
            if(sleep_time > 0)
                Util.sleep(sleep_time, busy_sleep);
        }
        System.out.println("done sending " + num_msgs + " to " + destination);
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
        Vector mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(channel.getAddress()))
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
        String props=null;


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
            help();
            return;
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
                           "[-exit_on_end] [-busy-sleep]");
    }


    public abstract static class Data implements Streamable {
    }

    public static class StartData extends Data {
        long num_values=0;

        public StartData() {}

        StartData(long num_values) {
            this.num_values=num_values;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeLong(num_values);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
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


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeLong(value);
            if(buf != null) {
                out.writeInt(buf.length);
                out.write(buf, 0, buf.length);
            }
            else {
                out.writeInt(0);
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            value=in.readLong();
            int len=in.readInt();
            if(len > 0) {
                buf=new byte[len];
                in.read(buf, 0, len);
            }
        }
    }


    private class MyReceiver extends ReceiverAdapter {
        private Data data;
        private boolean started=false;
        private long start=0, stop=0;
        private long current_value=0, tmp=0, num_values=0;
        private long total_time=0, msgs_per_sec;


        public void receive(Message msg) {
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
                    if(current_value % (num_values / 10) == 0)
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

        public void viewAccepted(View new_view) {
            System.out.println("** view: " + new_view);
        }
    }

}
