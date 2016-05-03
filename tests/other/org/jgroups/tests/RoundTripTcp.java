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
    int num=1000;
    int msg_size=10;
    boolean server=false;
    final byte[] RSP_BUF=new byte[]{1}; // 1=response
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
                        int b=in.read();
                        out.write(RSP_BUF, 0, RSP_BUF.length);
                    }
                    catch(Exception ex) {
                        // ex.printStackTrace();
                        Util.close(in);
                        Util.close(out);
                        client_sock.close();
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
        long   start, stop, total;
        double requests_per_sec;
        double    ms_per_req;
        int     print=num / 10;

        num_responses=0;
        for(int i=0; i < buf.length; i++) {
            buf[i]=0; // 0=request
        }


        OutputStream out=null;
        InputStream in=null;

        try {
            out=sock.getOutputStream();
            in=sock.getInputStream();
            start=System.currentTimeMillis();
            for(int i=0; i < num; i++) {
                try {
                    out.write(buf, 0, buf.length);
                    int b=in.read();
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
            stop=System.currentTimeMillis();
            total=stop-start;
            requests_per_sec=num / (total / 1000.0);
            ms_per_req=total / (double)num;
            System.out.println("Took " + total + "ms for " + num + " requests: " + requests_per_sec +
                    " requests/sec, " + ms_per_req + " ms/request");
        }
        finally {
            Util.close(in);
            Util.close(out);
            sock.close();
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
        System.out.println("RoundTripTcp [-server] [-host <host>] [-port <port>]");
    }
}
