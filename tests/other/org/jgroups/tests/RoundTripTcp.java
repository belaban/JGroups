package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Bits;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that measure RTT between a client and server using ServerSocket/Socket
 * @author Bela Ban
 */
public class RoundTripTcp {
    protected int                    num_msgs=20000;
    protected int                    num_senders=1; // number of sender threads
    protected boolean                details;
    protected static final byte      REQ=0, RSP=1;
    protected Sender[]               senders;
    protected Socket                 sock;
    protected OutputStream           output;
    protected InputStream            input;
    protected Receiver               receiver_thread;


    private void start(InetAddress host, int port, boolean server) throws IOException {
        if(server) { // simple single threaded server, can only handle a single connection at a time
            ServerSocket srv_sock=new ServerSocket(port, 50, host);
            System.out.println("server started (ctrl-c to kill)");
            for(;;) {
                Socket client_sock=srv_sock.accept();
                client_sock.setTcpNoDelay(true); // we're concerned about latency
                byte[] buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                InputStream in=client_sock.getInputStream(); OutputStream out=client_sock.getOutputStream();
                while(true) {
                    try {
                        int num=in.read(buf, 0, buf.length);
                        if(num == -1)
                            break;
                        if(num != buf.length)
                            throw new IllegalStateException("expected " + buf.length + " bytes, but only got " + num);
                        byte type=buf[0];
                        short id=Bits.readShort(buf, 1);
                        switch(type) {
                            case REQ:
                                byte[] rsp_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                                rsp_buf[0]=RSP;
                                Bits.writeShort(id, rsp_buf, 1);
                                out.write(rsp_buf, 0, rsp_buf.length);
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
                Util.close(client_sock, in, out);
            }
        }
        else {
            sock=new Socket();
            try {
                sock.setTcpNoDelay(true);
                sock.connect(new InetSocketAddress(host, port));
                output=sock.getOutputStream();
                input=sock.getInputStream();
                receiver_thread=new Receiver(input);
                receiver_thread.start();
                loop();
            }
            finally {
                Util.close(sock);
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
        protected final byte[]           req_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE]; // request and id
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final AverageMinMax    avg=new AverageMinMax(); // in ns

        public Sender(short id, CountDownLatch latch, AtomicInteger sent_msgs) {
            this.id=id;
            this.latch=latch;
            this.sent_msgs=sent_msgs;
            req_buf[0]=REQ;
            Bits.writeShort(id, req_buf, 1); // writes id at buf_reqs[1-2]
            print=Math.max(1, num_msgs / 10);
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
                    output.write(req_buf, 0, req_buf.length);
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
        protected final InputStream in;

        public Receiver(InputStream in) {
            this.in=in;
        }

        public void run() {
            byte[] buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
            for(;;) {
                try {
                    int num=in.read(buf, 0, buf.length);
                    if(num == -1)
                        return;
                    if(num != buf.length)
                        throw new IllegalStateException("expected " + buf.length + " bytes, but got only " + num);
                    byte type=buf[0];
                    switch(type) {
                        case RSP:
                            short id=Bits.readShort(buf, 1);
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
        boolean     server=false;
        InetAddress host=null;
        int         port=7500;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-server")) {
                server=true;
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

        new RoundTripTcp().start(host, port, server);
    }



    private static void help() {
        System.out.println("RoundTripTcp [-server] [-host <host>] [-port <port>]");
    }
}
