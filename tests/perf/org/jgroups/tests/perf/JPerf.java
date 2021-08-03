package org.jgroups.tests.perf;

import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tool to measure TCP throughput, similar to iperf
 * @author Bela Ban
 */
public class JPerf {
    protected int              size=1000;
    protected static final int STATS_INTERVAL=6000;

    private void start(boolean sender, int size, String bind_addr, String host, int port,
                       int receivebuf, int sendbuf) throws IOException {
        this.size=size;

        if(sender) {
            System.out.println("-- creating socket to " + host + ":" + port);
            Socket sock=new Socket(host, port);
            if(receivebuf > 0)
                sock.setReceiveBufferSize(receivebuf);
            if(sendbuf > 0)
                sock.setSendBufferSize(sendbuf);
            OutputStream out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream(), 65000));
            byte[] buf=new byte[size];

            LongAdder sent=new LongAdder();
            Runnable stats=new Stats(sent, true, size);
            Thread stats_thread=new Thread(stats, "sender-stats");
            stats_thread.start();
            for(;;) {
                out.write(buf, 0, buf.length);
                sent.increment();
            }
        }
        else {
            InetAddress bind=InetAddress.getByName(bind_addr);
            ServerSocket srv_sock=new ServerSocket(port, 1, bind);
            if(receivebuf > 0)
                srv_sock.setReceiveBufferSize(receivebuf); // needs to be set before accept, so the client sock has the same recvbuf!
            System.out.printf("-- listening on %s\n", srv_sock.getLocalSocketAddress());
            Socket client_sock=srv_sock.accept();
            if(receivebuf > 0)
                client_sock.setReceiveBufferSize(receivebuf);
            if(sendbuf > 0)
                client_sock.setSendBufferSize(sendbuf);
            System.out.printf("-- accepted connection from %s\n", client_sock.getRemoteSocketAddress());
            DataInputStream in=new DataInputStream(new BufferedInputStream(client_sock.getInputStream()));
            byte[] buf=new byte[size];
            LongAdder received=new LongAdder();
            Runnable stats=new Stats(received, false, size);
            Thread stats_thread=new Thread(stats, "receiver-stats");
            stats_thread.start();
            for(;;) {
                in.readFully(buf, 0, buf.length);
                received.increment();
            }
        }
    }


    protected static class Stats implements Runnable {
        protected final LongAdder cnt;
        protected final boolean   sender;
        protected final int       msg_size;

        public Stats(LongAdder cnt, boolean sender, int msg_size) {
            this.cnt=cnt;
            this.sender=sender;
            this.msg_size=msg_size;
        }

        public void run() {
            for(;;) {
                long msgs_before=cnt.sum();
                Util.sleep(STATS_INTERVAL);
                long msgs_after=cnt.sumThenReset();
                if(msgs_after > msgs_before) {
                    long msgs=msgs_after-msgs_before, bytes=msgs * msg_size;
                    double msgs_per_sec=msgs / (STATS_INTERVAL / 1000.0), bytes_per_sec=bytes/(STATS_INTERVAL/1000.0);
                    System.out.printf("-- %s %,.2f msgs/sec %s/sec\n",
                                      sender? "sent" : "received", msgs_per_sec, Util.printBytes(bytes_per_sec));
                }
            }
        }
    }


    protected static void help() {
        System.out.println("JPerf [-help] [-sender] [-size <bytes>] [-bind_addr <interface>] " +
                             "[-host <IP addr>] [-port <port>] [-receivebuf <bytes>] [-sendbuf <bytes>]");
    }

    public static void main(String[] args) throws IOException {
        boolean sender=false;
        String bind_addr="127.0.0.1";
        int port=10000;
        int size=1000;
        int receivebuf=0, sendbuf=0;
        String host="127.0.0.1";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equals("-size")) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-bind_addr")) {
                bind_addr=args[++i];
                continue;
            }
            if(args[i].equals("-host")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-receivebuf")) {
                receivebuf=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-sendbuf")) {
                sendbuf=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }
        JPerf p=new JPerf();
        p.start(sender, size, bind_addr, host, port, receivebuf, sendbuf);
    }



}
