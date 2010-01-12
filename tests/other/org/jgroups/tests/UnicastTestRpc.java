
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
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
    private boolean exit_on_end=false, busy_sleep=false, sync=false, oob=false;
    private int num_threads=1;
    private int num_msgs=100000, msg_size=1000;

    private boolean started=false;
    private long start=0, stop=0;
    private AtomicInteger current_value=new AtomicInteger(0);
    private int num_values=0, print;
    private long msgs_per_sec, total_time=0;
    private AtomicLong total_bytes=new AtomicLong(0);

    private static final Method START;
    private static final Method RECEIVE;
    private static final Method[] METHODS=new Method[2];

    static {
        try {
            START=UnicastTestRpc.class.getMethod("startTest", int.class);
            RECEIVE=UnicastTestRpc.class.getMethod("receiveData", int.class, byte[].class);
            METHODS[0]=START;
            METHODS[1]=RECEIVE;
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, long sleep_time, boolean exit_on_end, boolean busy_sleep, boolean sync, boolean oob) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        this.sync=sync;
        this.oob=oob;
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

    void stop() {
        if(disp != null)
            disp.stop();
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void startTest(int num_values) {
        if(started) {
            System.err.println("UnicastTest.run(): received START data, but am already processing data");
        }
        else {
            started=true;
            current_value.set(0); // first value to be received
            total_bytes.set(0);
            this.num_values=num_values;
            print=num_values / 10;
            start=System.currentTimeMillis();
        }
    }

    public void receiveData(int value, byte[] buffer) {
        long new_val=current_value.incrementAndGet();
        total_bytes.addAndGet(buffer.length);
        if(print > 0 && new_val % print == 0)
            System.out.println("received " + current_value);
        if(new_val >= num_values) {
            stop=System.currentTimeMillis();
            total_time=stop - start;
            msgs_per_sec=(long)(num_values / (total_time / 1000.0));
            double throughput=total_bytes.get() / (total_time / 1000.0);
            System.out.println("-- received " + num_values + " messages in " + total_time +
                    " ms (" + msgs_per_sec + " messages/sec, " + Util.printBytes(throughput) + " / sec)");
            started=false;
            if(exit_on_end)
                System.exit(0);
        }
    }


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print conns " +
                    "[4] Trash conn [5] Trash all conns" +
                    "\n[6] Set sender threads (" + num_threads + ") [7] Set num msgs (" + num_msgs + ") " +
                    "[8] Set msg size (" + Util.printBytes(msg_size) + ")" +
                    "\n[o] Toggle OOB (" + oob + ") [s] Toggle sync (" + sync + ")" +
                    "\n[q] Quit\n");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                try {
                    invokeRpcs();
                }
                catch(Throwable t) {
                    System.err.println(t);
                }
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
            case 's':
                sync=!sync;
                System.out.println("sync=" + sync);
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
        Address destination=getReceiver();

        if(num_threads > 1 && num_msgs % num_threads != 0) {
            System.err.println("num_msgs (" + num_msgs + " ) has to be divisible by num_threads (" + num_threads + ")");
            return;
        }

        if(destination == null) {
            System.err.println("UnicastTest.invokeRpcs(): receiver is null, cannot send messages");
            return;
        }

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) + " on " +
                destination + ", sync=" + sync + ", oob=" + oob);
        
        // The first call needs to be synchronous with OOB !
        RequestOptions options=new RequestOptions(GroupRequest.GET_ALL, 0, false, null);
        if(sync) options.setFlags(Message.DONT_BUNDLE);
        if(oob) options.setFlags(Message.OOB);

        disp.callRemoteMethod(destination, new MethodCall((short)0, new Object[]{num_msgs}), options);
        options.setMode(sync? GroupRequest.GET_ALL : GroupRequest.GET_NONE);

        Invoker[] invokers=new Invoker[num_threads];
        for(int i=0; i < invokers.length; i++)
            invokers[i]=new Invoker(destination, options, num_msgs / num_threads);
        for(Invoker invoker: invokers)
            invoker.start();
        for(Invoker invoker: invokers)
            invoker.join();

        System.out.println("done sending " + num_msgs + " to " + destination);
    }

    void setSenderThreads() throws Exception {
        int threads=Util.readIntFromStdin("Number of sender threads: ");
        int old=this.num_threads;
        this.num_threads=threads;
        System.out.println("sender threads set to " + num_threads + " (from " + old + ")");
    }

    void setNumMessages() throws Exception {
        num_msgs=Util.readIntFromStdin("Number of RPCs: ");
        System.out.println("Set num_msgs=" + num_msgs);
        print=num_msgs / 10;
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

    private class Invoker extends Thread {
        private final Address destination;
        private final RequestOptions options;
        private final int number_of_msgs;

        
        public Invoker(Address destination, RequestOptions options, int number_of_msgs) {
            this.destination=destination;
            this.options=options;
            this.number_of_msgs=number_of_msgs;
        }

        public void run() {
            byte[] buf=new byte[msg_size];
            Object[] args=new Object[]{0, buf};
            MethodCall call=new MethodCall((short)1, args);

            for(int i=1; i <= number_of_msgs; i++) {
                args[0]=i;
                try {
                    disp.callRemoteMethod(destination, call, options);
                    if(print > 0 && i % print == 0)
                        System.out.println("-- invoked " + i);
                    if(sleep_time > 0)
                        Util.sleep(sleep_time, busy_sleep);
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }
        }
    }


    static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public byte[] objectToByteBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            if(call.getId() == 0) {
                Integer arg=(Integer)call.getArgs()[0];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE);
                buf.put((byte)0).putInt(arg);
                return buf.array();
            }
            else if(call.getId() == 1) {
                Integer arg=(Integer)call.getArgs()[0];
                byte[] arg2=(byte[])call.getArgs()[1];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + Global.INT_SIZE + arg2.length);
                buf.put((byte)1).putInt(arg).putInt(arg2.length).put(arg2, 0, arg2.length);
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
                    int arg=buf.getInt();
                    return new MethodCall((short)0, new Object[]{arg});
                case 1:
                    arg=buf.getInt();
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
        boolean sync=false;
        boolean oob=false;


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
            if("-sync".equals(args[i])) {
                sync=true;
                continue;
            }
            if("-oob".equals(args[i])) {
                oob=true;
                continue;
            }
            help();
            return;
        }

        UnicastTestRpc  test=null;
        try {
            test=new UnicastTestRpc();
            test.init(props, sleep_time, exit_on_end, busy_sleep, sync, oob);
            test.eventLoop();
        }
        catch(Throwable ex) {
            System.err.println(ex);
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UnicastTestRpc [-help] [-props <props>] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep]");
    }


}