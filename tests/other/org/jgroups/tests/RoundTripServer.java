package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Bits;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class that measure RTT between a client and server using {@link org.jgroups.blocks.cs.TcpServer} or
 * {@link org.jgroups.blocks.cs.NioServer}. Can be used to compare latency to {@link RoundTripTcp}.
 * @author Bela Ban
 */
public class RoundTripServer {
    protected int                    num_msgs=20000;
    protected int                    num_senders=1; // number of sender threads
    protected boolean                details;
    protected static final byte      REQ=0, RSP=1;
    protected Sender[]               senders;
    protected BaseServer             srv, client;
    protected ServerHandler          srv_handler=new ServerHandler();
    protected ClientHandler          client_handler=new ClientHandler();



    protected class ServerHandler extends ReceiverAdapter {
        public void receive(Address sender, byte[] buf, int offset, int length) {
            byte type=buf[0];
            short id=Bits.readShort(buf, 1);
            switch(type) {
                case REQ:
                    byte[] rsp_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE];
                    rsp_buf[0]=RSP;
                    Bits.writeShort(id, rsp_buf, 1);
                    try {
                        srv.send(sender, rsp_buf, 0, rsp_buf.length);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case RSP:
                    throw new IllegalStateException("server should not receive response");
                default:
                    throw new IllegalArgumentException("first byte needs to be either REQ or RSP but not " + type);
            }
        }
    }

    protected class ClientHandler extends ReceiverAdapter {
        public void receive(Address sender, byte[] buf, int offset, int length) {
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
    }


    protected void start(InetAddress host, int port, boolean server, boolean nio) throws Exception {
        if(server) {
            srv=nio? new NioServer(host, port) : new TcpServer(host, port);
            srv.connExpireTimeout(0);
            srv.tcpNodelay(true);
            srv.receiver(srv_handler); // handles requests
            srv.start();
        }
        else {
            client=nio? new NioClient(null, 0, host, port) : new TcpClient(null, 0, host, port);
            client.tcpNodelay(true);
            client.receiver(client_handler);
            client.start();
            try {
                loop(new IpAddress(host, port));
            }
            finally {
                Util.close(client);
            }
        }
    }


    protected void loop(Address host) {
        boolean looping=true;
        while(looping) {
            int c=Util.keyPress(String.format("[1] send [2] num_msgs (%d) [3] senders (%d)\n" +
                                                "[d] details (%b) [x] exit\n", num_msgs, num_senders, details));
            try {
                switch(c) {
                    case '1':
                        sendRequests(host);
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


    protected void sendRequests(Address dest) throws Exception {
        final CountDownLatch latch=new CountDownLatch(num_senders +1);
        final AtomicInteger sent_msgs=new AtomicInteger(0);
        senders=new Sender[num_senders];
        for(int i=0; i < num_senders; i++) {
            senders[i]=new Sender((short)i, dest, latch, sent_msgs);
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
        protected final Address          dest;
        protected final byte[]           req_buf=new byte[Global.BYTE_SIZE + Global.SHORT_SIZE]; // request and id
        protected final CountDownLatch   latch;
        protected final AtomicInteger    sent_msgs; // current number of messages; senders stop if sent_msgs >= num_msgs
        protected final Promise<Boolean> promise=new Promise<>(); // for the sender thread to wait for the response
        protected final int              print;
        protected final AverageMinMax    avg=new AverageMinMax(); // in ns

        public Sender(short id, Address dest, CountDownLatch latch, AtomicInteger sent_msgs) {
            this.id=id;
            this.dest=dest;
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
                    client.send(dest, req_buf, 0, req_buf.length);
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



    public static void main(String[] args) throws Exception {
        boolean     server=false, nio=false;
        InetAddress host=null;
        int         port=7500;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-server")) {
                server=true;
                continue;
            }
            if(args[i].equals("-nio")) {
                nio=Boolean.parseBoolean(args[++i]);
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

        new RoundTripServer().start(host, port, server, nio);
    }



    private static void help() {
        System.out.println("RoundTripServer [-server] [-host <host>] [-port <port>] [-nio true|false]");
    }
}
