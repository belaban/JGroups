
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Vector;
import java.nio.ByteBuffer;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTestRpc extends ReceiverAdapter {
    private JChannel channel;
    private RpcDispatcher disp;
    static final String groupname="UnicastTest-Group";
    private long sleep_time=0;
    private boolean exit_on_end=false, busy_sleep=false;

    private boolean started=false;
    private long start=0, stop=0;
    private long current_value=0, num_values=0;
    private long total_time=0, msgs_per_sec;

    private static final Method START;
    private static final Method RECEIVE;
    private static final Method[] METHODS=new Method[2];

    static {
        try {
            START=UnicastTestRpc.class.getMethod("startTest", long.class);
            RECEIVE=UnicastTestRpc.class.getMethod("receiveData", long.class, byte[].class);
            METHODS[0]=START;
            METHODS[1]=RECEIVE;
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, long sleep_time, boolean exit_on_end, boolean busy_sleep) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        channel=new JChannel(props);
        disp=new RpcDispatcher(channel, null, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return METHODS[id];
            }
        });
        disp.setRequestMarshaller(new CustomMarshaller());
        channel.connect(groupname);

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void startTest(long num_values) {
        if(started) {
            System.err.println("UnicastTest.run(): received START data, but am already processing data");
        }
        else {
            started=true;
            current_value=0; // first value to be received
            this.num_values=num_values;
            start=System.currentTimeMillis();
        }
    }

    public void receiveData(long value, byte[] buffer) {
        if(current_value + 1 != value) {
            System.err.println("-- message received (" + value + ") is not 1 greater than " + current_value);
            return;
        }
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


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print connections " +
                    "[4] Trash connection [5] Trash all connections [q] Quit ");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                invokeRpcs();
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


    void invokeRpcs() throws Throwable {
        long num_msgs=Util.readLongFromStdin("Number of RPCs: ");
        int msg_size=Util.readIntFromStdin("Message size: ");
        Address destination=getReceiver();

        if(destination == null) {
            System.err.println("UnicastTest.invokeRpcs(): receiver is null, cannot send messages");
            return;
        }

        System.out.println("invoking " + num_msgs + " RPCs on " + destination);

        disp.callRemoteMethod(destination, new MethodCall((short)0, new Object[]{num_msgs}), GroupRequest.GET_NONE, 5000);

        int print=(int)(num_msgs / 10);
        byte[] buf=new byte[msg_size];
        for(int i=1; i <= num_msgs; i++) {
            disp.callRemoteMethod(destination, new MethodCall((short)1, new Object[]{(long)i, buf}), GroupRequest.GET_NONE, 5000);
            if(i % print == 0)
                System.out.println("-- invoked " + i);
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
        try {
            Vector<Address> mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(channel.getAddress()))
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i) + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i));
            }
            System.out.flush();
            System.in.skip(System.in.available());
            BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
            String str=reader.readLine().trim();
            int index=Integer.parseInt(str);
            return mbrs.elementAt(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }


    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public byte[] objectToByteBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            if(call.getId() == 0) {
                Long arg=(Long)call.getArgs()[0];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE);
                buf.put((byte)0).putLong(arg);
                return buf.array();
            }
            else if(call.getId() == 1) {
                Long arg=(Long)call.getArgs()[0];
                byte[] arg2=(byte[])call.getArgs()[1];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.LONG_SIZE + Global.INT_SIZE + arg2.length);
                buf.put((byte)1).putLong(arg).putInt(arg2.length).put(arg2, 0, arg2.length);
                return buf.array();
            }
            else
                throw new IllegalStateException("method " + call.getMethod() + " not known");
        }

        public Object objectFromByteBuffer(byte[] buffer) throws Exception {
            ByteBuffer buf=ByteBuffer.wrap(buffer);

            byte type=buf.get();
            switch(type) {
                case 0:
                    long arg=buf.getLong();
                    return new MethodCall((short)0, new Object[]{arg});
                case 1:
                    arg=buf.getLong();
                    int len=buf.getInt();
                    byte[] arg2=new byte[len];
                    buf.get(arg2, 0, arg2.length);
                    return new MethodCall((short)1, new Object[]{arg, arg2});
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
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
            UnicastTestRpc test=new UnicastTestRpc();
            test.init(props, sleep_time, exit_on_end, busy_sleep);
            test.eventLoop();
        }
        catch(Throwable ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-props <props>] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep]");
    }


}