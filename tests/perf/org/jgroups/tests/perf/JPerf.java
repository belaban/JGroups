package org.jgroups.tests.perf;

import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tool to measure TCP throughput, similar to iperf
 * @author Bela Ban
 */
public class JPerf {

    private static void start(boolean client, int length, String bind_addr, String host, int port,
                              int window_size, int interval) throws IOException {
        if(client) {
            System.out.println("-- creating socket to " + host + ":" + port);
            Socket sock=new Socket(host, port);
            if(window_size > 0) {
                sock.setReceiveBufferSize(window_size);
                sock.setSendBufferSize(window_size);
            }
            OutputStream out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream(), 65535));
            byte[] buf=new byte[length];

            LongAdder sent=new LongAdder();
            Runnable stats=new Stats(sent, true, length, interval*1000);
            Thread stats_thread=new Thread(stats, "sender-stats");
            stats_thread.start();
            for(;;) {
                out.write(buf, 0, buf.length);
                sent.increment();
            }
        }
        else {
            InetAddress bind=InetAddress.getByName(bind_addr);
            SocketAddress saddr=new InetSocketAddress(bind, port);
            ServerSocket srv_sock=new ServerSocket();
            if(window_size > 0)
                srv_sock.setReceiveBufferSize(window_size);
            srv_sock.bind(saddr, 1);

            System.out.printf("------------------------------------------------------------\n" +
                                "Server listening on TCP port %d\n" +
                                "TCP window size:  %s\n" +
                                "Length of buffer (to send or receive): %s\n" +
                                "------------------------------------------------------------\n",
                              srv_sock.getLocalPort(), Util.printBytes(window_size), Util.printBytes(length));
            System.out.printf("-- listening on %s\n", srv_sock.getLocalSocketAddress());
            for(;;) {
                Socket client_sock=srv_sock.accept();
                if(window_size > 0) {
                    client_sock.setReceiveBufferSize(window_size);
                    client_sock.setSendBufferSize(window_size);
                }
                System.out.printf("-- accepted connection from %s\n", client_sock.getRemoteSocketAddress());
                DataInputStream in=new DataInputStream(new BufferedInputStream(client_sock.getInputStream(), 65535));
                byte[] buf=new byte[length];
                LongAdder received=new LongAdder();
                Runnable stats=new Stats(received, false, length, interval*1000);
                Thread stats_thread=new Thread(stats, "receiver-stats");
                stats_thread.start();
                for(;;) {
                    try {
                        in.readFully(buf, 0, buf.length);
                        received.increment();
                    }
                    catch(Exception ex) {
                        System.out.printf("client %s closed connection: %s\n", client_sock.getRemoteSocketAddress(), ex);
                        break;
                    }
                }
            }
        }
    }


    protected static class Stats implements Runnable {
        protected final LongAdder cnt;
        protected final boolean   sender;
        protected final int       msg_size;
        protected final int       interval;

        public Stats(LongAdder cnt, boolean sender, int msg_size, int interval) {
            this.cnt=cnt;
            this.sender=sender;
            this.msg_size=msg_size;
            this.interval=interval;
        }

        public void run() {
            for(;;) {
                long msgs_before=cnt.sum();
                Util.sleep(interval);
                long msgs_after=cnt.sumThenReset();
                if(msgs_after > msgs_before) {
                    long msgs=msgs_after-msgs_before, bytes=msgs * msg_size;
                    double msgs_per_sec=msgs / (interval / 1000.0), bytes_per_sec=bytes/(interval/1000.0);
                    System.out.printf("-- %s %,.2f msgs/sec %s/sec\n",
                                      sender? "sent" : "received", msgs_per_sec, Util.printBytes(bytes_per_sec));
                }
            }
        }
    }


    protected static void help() {
        System.out.println("JPerf [-help] [-c host] [-s] [-l <bytes>] [-B <bind address>] " +
                             "[-port <port>] [-w <bytes>] " +
                             "[-i <interval in secs>\n" +
                             "-c: client\n-s: server\n-w: TCP window size (socket buffer size)\n" +
                             "-l: length of buffer to write or read (in bytes)");
    }

    public static void main(String[] args) throws IOException {
        boolean client=false;
        String bind_addr="127.0.0.1";
        int port=5001;
        int length=128000;
        int window_size=0, interval=2;
        String host="127.0.0.1";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-c")) {
                client=true;
                host=args[++i];
                continue;
            }
            if(args[i].equals("-s")) {
                client=false;
                continue;
            }
            if(args[i].equals("-l")) {
                length=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-B")) {
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
            if(args[i].equals("-w")) {
                window_size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-i")) {
                interval=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }
        JPerf.start(client, length, bind_addr, host, port, window_size, interval);
    }



}
