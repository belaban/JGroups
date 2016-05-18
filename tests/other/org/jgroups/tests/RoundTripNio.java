package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Class that measure RTT between a client and server using NIO.2 {@link java.nio.channels.ServerSocketChannel} and
 * {@link java.nio.channels.SocketChannel}. Can be configured to use direct or heap-based buffers.
 * @author Bela Ban
 */
public class RoundTripNio {
    protected int                    num_msgs=20000;
    protected int                    num_senders=1; // number of sender threads
    protected boolean                details;
    protected static final byte      REQ=0, RSP=1;
    protected Sender[]               senders;
    protected SocketChannel          client_channel;
    protected Receiver               receiver_thread;
    protected boolean                direct_buffers; // whether or not to use direct byte buffers


    private void start(InetAddress host, int port, boolean server, boolean direct) throws IOException {
        this.direct_buffers=direct;
        if(server) { // simple single threaded server, can only handle a single connection at a time
            ServerSocketChannel srv_channel=ServerSocketChannel.open();
            srv_channel.bind(new InetSocketAddress(host, port), 50);
            System.out.println("server started (ctrl-c to kill)");
            int size=Global.BYTE_SIZE + Global.SHORT_SIZE;
            ByteBuffer buf=direct_buffers? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            ByteBuffer rsp_buf=direct_buffers? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            rsp_buf.put(0, RSP); // rsp_buf[0] is always a RSP byte

            for(;;) {
                SocketChannel client_ch=srv_channel.accept();
                client_ch.socket().setTcpNoDelay(true); // we're concerned about latency
                while(true) {
                    try {
                        buf.position(0);
                        int num=client_ch.read(buf);
                        if(num == -1)
                            break;
                        if(num != size)
                            throw new IllegalStateException("expected " + size + " bytes, but only got " + num);
                        byte type=buf.get(0);
                        short id=buf.getShort(1);
                        switch(type) {
                            case REQ:
                                rsp_buf.putShort(1, id).position(0);
                                client_ch.write(rsp_buf);
                                break;
                            case RSP:
                                throw new IllegalStateException("server should not receive response");
                            default:
                                throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + type);
                        }
                    }
                    catch(Exception ex) {
                        ex.printStackTrace();
                        break;
                    }
                }
                Util.close(client_ch, srv_channel);
            }
        }
        else {
            client_channel=SocketChannel.open();
            try {
                client_channel.socket().setTcpNoDelay(true);
                client_channel.connect(new InetSocketAddress(host, port));
                receiver_thread=new Receiver();
                receiver_thread.start();
                loop();
            }
            finally {
                Util.close(client_channel);
            }
        }
    }


    protected void loop() {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d)\n" +
                                                "[d] details (%b) [x] exit\n", num_msgs, num_senders, details));
            try {
                switch(c) {
                    case '1':
                        sendRequests();
                        break;
                    case '2':
                        num_msgs=Util.readIntFromStdin("num_msgs: ");
                        break;
                    case '3':
                        num_senders=Util.readIntFromStdin("num_senders: ");
                        break;
                    case 'd':
                        details=!details;
                        break;
                    case 'x':
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }


    protected void sendRequests() throws Exception {
        final CountDownLatch latch=new CountDownLatch(num_senders +1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        senders=new Sender[num_senders];
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, latch, sent_msgs);
            senders[i].start();
        }

        long start=System.nanoTime();
        latch.countDown(); // start all sender threads
        for(Sender sender: senders)
            sender.join();
        long total_time=System.nanoTime()-start;
        double msgs_sec=num_msgs / (total_time / 1_000_000_000.0);

        AverageMinMax avg=null;
        if(details)
            System.out.println("");
        for(Sender sender: senders) {
            if(details)
                System.out.printf("%d: %s\n", sender.id, print(sender.avg));
            if(avg == null)
                avg=sender.avg;
            else
                avg.merge(sender.avg);
        }

        System.out.printf(Util.bold("\n\nreqs/sec = %.2f, " +
                                      "round-trip = min/avg/max: %.2f / %.2f / %.2f us\n\n"),
                          msgs_sec, avg.min()/1000.0, avg.average() / 1000.0, avg.max()/1000.0);
    }


    protected static String print(AverageMinMax avg) {
        return String.format("round-trip min/avg/max = %.2f / %.2f / %.2f us",
                             avg.min() / 1000.0, avg.average() / 1000.0, avg.max() / 1000.0);
    }

    protected class Sender extends Thread {
        protected final short            id;
        protected final ByteBuffer       req_buf;
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final AverageMinMax    avg=new AverageMinMax(); // in ns

        public Sender(short id, CountDownLatch latch, AtomicInteger sent_msgs) {
            this.id=id;
            this.latch=latch;
            this.sent_msgs=sent_msgs;
            print=Math.max(1, num_msgs / 10);
            int required_size=Global.BYTE_SIZE + Global.SHORT_SIZE; // request and id
            req_buf=direct_buffers? ByteBuffer.allocateDirect(required_size) : ByteBuffer.allocate(required_size);
            req_buf.put(REQ).putShort(id);
        }

        public void run() {
            latch.countDown();
            for(;;) {
                int num=sent_msgs.incrementAndGet();
                if(num > num_msgs)
                    break;
                if(num > 0 && num % print == 0)
                    System.out.printf(".");
                promise.reset(false);
                try {
                    long start=System.nanoTime();
                    req_buf.position(0);
                    client_channel.write(req_buf);
                    promise.getResult(0);
                    long time=System.nanoTime()-start;
                    avg.add(time);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected class Receiver extends Thread {

        public void run() {
            int size=Global.BYTE_SIZE + Global.SHORT_SIZE; // request and id
            ByteBuffer rsp_buf=direct_buffers? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            for(;;) {
                try {
                    rsp_buf.position(0);
                    int num=client_channel.read(rsp_buf);
                    if(num == -1)
                        return;
                    if(num != size)
                        throw new IllegalStateException("expected " + size + " bytes, but got only " + num);
                    byte type=rsp_buf.get(0); // read type at position 0
                    switch(type) {
                        case RSP:
                            short id=rsp_buf.getShort(1);
                            senders[id].promise.setResult(true); // notify the sender of the response
                            break;
                        case REQ:
                            throw new IllegalStateException("client should not receive request");
                        default:
                            throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + type);
                    }
                }
                catch(Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


    public static void main(String[] args) throws IOException {
        boolean     server=false, direct=false;
        InetAddress host=null;
        int         port=7500;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-server")) {
                server=true;
                continue;
            }
            if(args[i].equals("-direct")) {
                direct=Boolean.valueOf(args[++i]);
                continue;
            }
            if(args[i].equals("-host")) {
                host=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        if(host == null)
            host=InetAddress.getLocalHost();

        new RoundTripNio().start(host, port, server, direct);
    }



    private static void help() {
        System.out.println("RoundTripNio [-server] [-host <host>] [-port <port>] [-direct true|false]");
    }
}
