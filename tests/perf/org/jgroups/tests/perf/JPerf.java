package org.jgroups.tests.perf;

import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.NumberFormat;

/**
 * Tool to measure TCP throughput, similar to iperf
 * @author Bela Ban
 */
public class JPerf {
    protected InetAddress local_addr, remote_addr;
    protected int         local_port, remote_port;
    protected int         num, size=1000, incr=1000;
    static  NumberFormat  f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }


    private void start(boolean sender, int num, int size, String local_addr, int local_port,
                       String remote_addr, int remote_port, int receivebuf, int sendbuf) throws IOException {
        this.num=num;
        this.size=size;
        this.local_addr=InetAddress.getByName(local_addr);
        this.local_port=local_port;
        this.remote_addr=InetAddress.getByName(remote_addr);
        this.remote_port=remote_port;

        incr=num / 10;

        if(sender) {
            System.out.println("-- creating socket to " + this.remote_addr + ":" + this.remote_port);
            Socket sock=new Socket(this.remote_addr, remote_port);
            sock.setReceiveBufferSize(receivebuf);
            sock.setSendBufferSize(sendbuf);
            DataOutputStream out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            byte[] buf=new byte[size];
            int cnt=1;
            System.out.println("-- sending " + num + " messages");
            for(int i=0; i < num; i++) {
                out.write(buf, 0, buf.length);
                out.flush();
                if(cnt % incr == 0)
                    System.out.println("-- sent " + cnt + " messages");
                cnt++;
            }
        }
        else {
            ServerSocket srv_sock=new ServerSocket(local_port, 10, this.local_addr);
            System.out.println("-- waiting for " + num + " messages on " + srv_sock.getLocalSocketAddress());
            Socket client_sock=srv_sock.accept();
            client_sock.setReceiveBufferSize(receivebuf);
            client_sock.setSendBufferSize(sendbuf);
            System.out.println("-- accepted connection from " + client_sock.getRemoteSocketAddress());
            DataInputStream in=new DataInputStream(new BufferedInputStream(client_sock.getInputStream()));
            byte[] buf=new byte[size];
            int counter=0;
            long received_bytes=0;
            long start=0;
            while(counter < num) {
                in.readFully(buf, 0, buf.length);
                received_bytes+=buf.length;
                if(start == 0)
                    start=System.currentTimeMillis();
                counter++;
                if(counter % incr == 0)
                    System.out.println("-- received " + counter);
            }
            long time=System.currentTimeMillis() - start;
            double msgs_sec=(counter / (time / 1000.0));
            double throughput=received_bytes / (time / 1000.0);
            System.out.println(String.format("\nreceived %d messages in %d ms (%.2f msgs/sec), throughput=%s",
                                             counter, time, msgs_sec, Util.printBytes(throughput)));
        }
    }



    static void help() {
        System.out.println("JPerf [-help] [-sender] [-num <number of msgs] [-size <bytes>] [-local_addr <interface>] [-local_port <port]" +
                "[-remote_addr <IP addr>] [-remote_port <port>] [-receivebuf <bytes>] [-sendbuf <bytes>]");
    }

    public static void main(String[] args) {
        boolean sender=false;
        int num=100000;
        String local_addr="127.0.0.1";
        int local_port=5000;
        int size=1000;
        int remote_port=10000;
        int receivebuf=200000, sendbuf=200000;
        String remote_addr="127.0.0.1";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-sender")) {
                sender=true;
                continue;
            }
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-size")) {
                size=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-local_addr")) {
                local_addr=args[++i];
                continue;
            }
            if(args[i].equals("-remote_addr")) {
                remote_addr=args[++i];
                continue;
            }
            if(args[i].equals("-local_port")) {
                local_port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-remote_port")) {
                remote_port=Integer.parseInt(args[++i]);
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
        try {
            new JPerf().start(sender, num, size, local_addr, local_port, remote_addr, remote_port, receivebuf, sendbuf);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }



}
