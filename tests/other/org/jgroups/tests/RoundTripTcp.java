package org.jgroups.tests;

import org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Class that measure RTT between a client and server using ServerSocket/Socket
 * @author Bela Ban
 */
public class RoundTripTcp {
    ServerSocket srv_sock;
    InetAddress host;
    int port=7500;
    int num=20000;
    int msg_size=10;
    boolean server=false;
    final byte[] RSP_BUF={1}; // 1=response
    int   num_responses=0;


    private void start(boolean server, int num, int msg_size, InetAddress host, int port) throws IOException {
        this.server=server;
        this.num=num;
        this.msg_size=msg_size;
        this.host=host;
        this.port=port;
        Socket client_sock;

        if(server) {
            srv_sock=new ServerSocket(port, 50, host);
            System.out.println("server started (ctrl-c to kill)");
            while(true) {
                client_sock=srv_sock.accept();
                while(true) {
                    InputStream in=null;
                    OutputStream out=null;
                    try {
                        in=client_sock.getInputStream();
                        out=client_sock.getOutputStream();
                        in.read();
                        out.write(RSP_BUF, 0, RSP_BUF.length);
                    }
                    catch(Exception ex) {
                        Util.close(in, out, client_sock);
                        break;
                    }
                }
            }
        }
        else {
            Socket sock=new Socket(host, port);
            System.out.println("sending " + num + " requests");
            sendRequests(sock);
        }
    }



    private void sendRequests(Socket sock) throws IOException {
        byte[] buf=new byte[msg_size];
        double requests_per_sec;
        int    print=num / 10;

        num_responses=0;
        for(int i=0; i < buf.length; i++)
            buf[i]=0; // 0=request

        OutputStream out=null;
        InputStream in=null;

        try {
            out=sock.getOutputStream();
            in=sock.getInputStream();
            long start=System.nanoTime();
            for(int i=0; i < num; i++) {
                try {
                    out.write(buf, 0, buf.length);
                    in.read();
                    num_responses++;
                    if(num_responses >= num) {
                        System.out.println("received all responses (" + num_responses + ")");
                        break;
                    }
                    if(num_responses % print == 0) {
                        System.out.println("- received " + num_responses);
                    }
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            long total=System.nanoTime()-start;
            requests_per_sec=num / (total / 1_000_000_000.0);
            double us_per_req=total / 1000.0 / (double)num;
            System.out.printf("\n%.2f ms for %d requests: %.2f reqs/sec, %.2f us/req\n\n",
                              total/1_000_000.0, num, requests_per_sec, us_per_req);
        }
        finally {
            Util.close(in, out, sock);
        }
    }


    public static void main(String[] args) throws IOException {
        boolean server=false;
        int num=100;
        int msg_size=10; // 10 bytes
        InetAddress host=null;
        int port=7500;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-server")) {
                server=true;
                continue;
            }
            if(args[i].equals("-size")) {
                msg_size=Integer.parseInt(args[++i]);
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
            RoundTripTcp.help();
            return;
        }

        if(host == null)
            host=InetAddress.getLocalHost();

        new RoundTripTcp().start(server, num, msg_size, host, port);
    }



    private static void help() {
        System.out.println("RoundTrip [-server] [-num <number of messages>] " +
                "[-size <size of each message (in bytes)>] [-host <host>] [-port <port>]");
    }
}
