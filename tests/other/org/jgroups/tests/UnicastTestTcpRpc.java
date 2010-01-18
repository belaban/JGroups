
package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Tests the UNICAST by invoking unicast RPCs between a sender and a receiver
 *
 * @author Bela Ban
 */
public class UnicastTestTcpRpc {
    private ServerSocket srv_sock;
    private volatile Socket sock;
    private DataInputStream sock_in;
    private DataOutputStream sock_out;

    private long sleep_time=0;
    private boolean exit_on_end=false, busy_sleep=false, sync=false, oob=false;
    private int num_threads=1;
    private int num_msgs=50000, msg_size=1000;

    private InetAddress addr=null;
    private int local_port=8000, dest_port=9000;

    private boolean started=false;
    private long start=0, stop=0;
    private AtomicInteger current_value=new AtomicInteger(0);
    private int num_values=0, print;
    private AtomicLong total_bytes=new AtomicLong(0);

    private Thread acceptor;

    private final byte[] buf=new byte[65535];
    long total_req_time=0, total_rsp_time=0, entire_req_time=0, num_entire_reqs=0;
    int num_reqs=0, num_rsps=0;

    static final byte START         =  0;
    static final byte RECEIVE_ASYNC =  1;
    static final byte RECEIVE_SYNC  =  2;
    static final byte ACK           = 10;


    public void init(long sleep_time, boolean exit_on_end, boolean busy_sleep, boolean sync, boolean oob,
                     String addr, int local_port, int dest_port) throws Exception {
        this.sleep_time=sleep_time;
        this.exit_on_end=exit_on_end;
        this.busy_sleep=busy_sleep;
        this.sync=sync;
        this.oob=oob;
        this.addr=InetAddress.getByName(addr);
        this.local_port=local_port;
        this.dest_port=dest_port;
        srv_sock=new ServerSocket(local_port);
        System.out.println("Listening on " + srv_sock.getLocalSocketAddress());
        
        acceptor=new Thread() {
            public void run() {
                while(true) {
                    Socket client_sock=null;
                    DataInputStream in=null;
                    DataOutputStream out=null;
                    try {
                        client_sock=srv_sock.accept();
                        set(client_sock);
                        in=new DataInputStream(client_sock.getInputStream());
                        out=new DataOutputStream(client_sock.getOutputStream());
                        if(!handleRequest(in, out)) {
                            Util.close(client_sock);
                        Util.close(out);
                        Util.close(in);
                        break;
                        }
                    }
                    catch(IOException e) {
                        Util.close(client_sock);
                        Util.close(out);
                        Util.close(in);
                        break;
                    }
                }
            }
        };
        acceptor.start();
    }

    void createSocket() throws IOException {
        if(sock == null) {
            sock=new Socket(addr, dest_port);
            set(sock);
            sock_in=new DataInputStream(sock.getInputStream());
            sock_out=new DataOutputStream(sock.getOutputStream());
        }
    }



    boolean handleRequest(DataInputStream in, DataOutputStream out) throws IOException {
           while(true) {
               byte type=(byte)in.read();
               if(type == -1)
                   return false;

               switch(type) {
                   case START:
                       int num=in.readInt();
                       startTest(num);
                       break;
                   case RECEIVE_ASYNC:
                   case RECEIVE_SYNC:
                       long val=in.readLong();
                       int len=in.readInt();
                       byte data[]=new byte[len];
                       in.readFully(data, 0, data.length);
                       receiveData(val, data);
                       if(type == RECEIVE_SYNC) {
                           out.writeLong(System.currentTimeMillis());
                           out.flush();
                       }
                       break;
                   default:
                       System.err.println("type " + type + " not known");
               }
           }
    }


    static void set(Socket socket) throws SocketException {
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(20000000);
        socket.setSendBufferSize(10000000);
    }


    void stop() {
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

            total_req_time=0; num_reqs=0; total_rsp_time=0; num_rsps=0; entire_req_time=0;
            num_entire_reqs=0;

            start=System.currentTimeMillis();
        }
    }

    public void receiveData(long value, byte[] buffer) {
        long diff=System.currentTimeMillis() - value;
        total_req_time+=diff;
        num_reqs++;

        long new_val=current_value.incrementAndGet();
        total_bytes.addAndGet(buffer.length);
        if(print > 0 && new_val % print == 0)
            System.out.println("received " + current_value);
        if(new_val >= num_values) {
            stop=System.currentTimeMillis();
            long total_time=stop - start;
            long msgs_per_sec=(long)(num_values / (total_time / 1000.0));
            double throughput=total_bytes.get() / (total_time / 1000.0);
            System.out.println("\n-- received " + num_values + " messages in " + total_time +
                    " ms (" + msgs_per_sec + " messages/sec, " + Util.printBytes(throughput) + " / sec)");



            double time_per_req=(double)total_req_time / num_reqs;
            System.out.println("received " + num_reqs + " requests in " + total_req_time + " ms, " + time_per_req +
                    " ms / req (only requests)\n");

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
                break;
            case '3':
                break;
            case '4':
                break;
            case '5':
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
                Util.close(sock);
                Util.close(srv_sock);
                return;
            default:
                break;
            }
        }
    }




    void invokeRpcs() throws Throwable {
        if(sock == null)
            createSocket();

        if(num_threads > 1 && num_msgs % num_threads != 0) {
            System.err.println("num_msgs (" + num_msgs + " ) has to be divisible by num_threads (" + num_threads + ")");
            return;
        }

        System.out.println("invoking " + num_msgs + " RPCs of " + Util.printBytes(msg_size) + " on " +
                ", sync=" + sync + ", oob=" + oob);

        num_entire_reqs=entire_req_time=total_rsp_time=num_rsps=0;

        sock_out.write(START);
        sock_out.writeInt(num_msgs);

        byte[] data=new byte[msg_size];
        byte type=sync? RECEIVE_SYNC : RECEIVE_ASYNC;
        for(int i=0; i < num_msgs; i++) {
            long tmp_start=System.currentTimeMillis();
            sock_out.write(type);
            sock_out.writeLong(tmp_start);
            sock_out.writeInt(msg_size);
            sock_out.write(data, 0, data.length);
            // sock_out.flush();
            if(sync) {
                long timestamp=sock_in.readLong();
                long curr_time=System.currentTimeMillis();
                long diff=curr_time - tmp_start;
                num_entire_reqs++;
                entire_req_time+=diff;
                diff=curr_time - timestamp;
                total_rsp_time+=diff;
                num_rsps++;
            }
        }
        sock_out.flush();

        System.out.println("done sending " + num_msgs + " to " + sock.getRemoteSocketAddress());

        double time_per_req=entire_req_time / (double)num_msgs;
            System.out.println("\ninvoked " + num_entire_reqs + " requests in " + entire_req_time + " ms: " + time_per_req +
                    " ms / req (entire request)");

        if(sync) {
                double time_per_rsp=total_rsp_time / (double)num_rsps;
                System.out.println("received " + num_rsps + " responses in " + total_rsp_time + " ms: " + time_per_rsp +
                        " ms / rsp (only response)\n");
        }
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





  /*  private class Invoker extends Thread {
        private final Address destination;
        private final RequestOptions options;
        private final int number_of_msgs;

        long total=0;

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
                args[0]=System.currentTimeMillis();
                try {
                    long start=System.currentTimeMillis();
                    disp.callRemoteMethod(destination, call, options);
                    long diff=System.currentTimeMillis() - start;
                    total+=diff;

                    if(print > 0 && i % print == 0)
                        System.out.println("-- invoked " + i);
                    if(sleep_time > 0)
                        Util.sleep(sleep_time, busy_sleep);
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                }
            }

            double time_per_req=total / (double)number_of_msgs;
            System.out.println("invoked " + number_of_msgs + " requests in " + total + " ms: " + time_per_req +
                    " ms / req");
        }
    }*/


   /* static class CustomMarshaller implements RpcDispatcher.Marshaller {

        public byte[] objectToByteBuffer(Object obj) throws Exception {
            MethodCall call=(MethodCall)obj;
            if(call.getId() == 0) {
                Integer arg=(Integer)call.getArgs()[0];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE);
                buf.put((byte)0).putInt(arg);
                return buf.array();
            }
            else if(call.getId() == 1) {
                Long arg=(Long)call.getArgs()[0];
                byte[] arg2=(byte[])call.getArgs()[1];
                ByteBuffer buf=ByteBuffer.allocate(Global.BYTE_SIZE + Global.INT_SIZE + Global.LONG_SIZE + arg2.length);
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
                    int arg=buf.getInt();
                    return new MethodCall((short)0, new Object[]{arg});
                case 1:
                    Long longarg=buf.getLong();
                    int len=buf.getInt();
                    byte[] arg2=new byte[len];
                    buf.get(arg2, 0, arg2.length);
                    return new MethodCall((short)1, new Object[]{longarg, arg2});
                default:
                    throw new IllegalStateException("type " + type + " not known");
            }
        }
    }*/


    public static void main(String[] args) {
        long sleep_time=0;
        boolean exit_on_end=false;
        boolean busy_sleep=false;
        boolean sync=false;
        boolean oob=false;

        String addr=null;
        int dest_port=9000, local_port=8000;


        for(int i=0; i < args.length; i++) {
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
            if("-addr".equals(args[i])) {
                addr=args[++i];
                continue;
            }
            if("-dest_port".equals(args[i])) {
                dest_port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-local_port".equals(args[i])) {
                local_port=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        UnicastTestTcpRpc test=null;
        try {
            test=new UnicastTestTcpRpc();
            test.init(sleep_time, exit_on_end, busy_sleep, sync, oob, addr, local_port, dest_port);
            test.eventLoop();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UnicastTestRpc [-help] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep] [-addr address] [-dest_port port] [-local_port port]");
    }


}