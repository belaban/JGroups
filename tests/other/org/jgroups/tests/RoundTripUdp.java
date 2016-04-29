package org.jgroups.tests;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Class that measure RTT between a client and server using datagram sockets
 * @author Bela Ban
 */
public class RoundTripUdp {
    DatagramSocket sock;
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

        if(server) {
            sock=new DatagramSocket(port, host);
            System.out.println("server started (ctrl-c to kill)");
            while(true) {
                byte[] buf=new byte[65000];
                DatagramPacket packet;
                packet=new DatagramPacket(buf, 0, buf.length);
                sock.receive(packet);
                packet=new DatagramPacket(RSP_BUF, 0, RSP_BUF.length, packet.getAddress(), packet.getPort());
                sock.send(packet); // send the response
            }
        }
        else {
            sock=new DatagramSocket();
            System.out.println("sending " + num + " requests");
            sendRequests();
        }
        sock.close();
    }



    private void sendRequests() {
        byte[] buf=new byte[msg_size];
        int     print=num / 10;

        num_responses=0;
        for(int i=0; i < buf.length; i++) {
            buf[i]=0; // 0=request
        }

        long start=System.nanoTime();
        for(int i=0; i < num; i++) {
            DatagramPacket packet=new DatagramPacket(buf, 0, buf.length, host, port);
            try {
                sock.send(packet);

                byte[] response=new byte[1];
                DatagramPacket rsp_packet=new DatagramPacket(response, 0, response.length);
                sock.receive(rsp_packet);
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
        long total=System.nanoTime() - start;
        double requests_per_sec=num / (total / 1000_000_000.0);
        double us_per_req=total / 1000.0 / (double)num;
        System.out.printf("\n%.2f ms for %d requests: %.2f reqs/sec, %.2f us/request\n\n",
                          total / 1000_000.0, num, requests_per_sec, us_per_req);
    }


    public static void main(String[] args) throws IOException {
        boolean server=false;
        int num=20000;
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
            RoundTripUdp.help();
            return;
        }

        if(host == null)
            host=InetAddress.getLocalHost();
        new RoundTripUdp().start(server, num, msg_size, host, port);
    }



    private static void help() {
        System.out.println("RoundTripUdp [-server] [-num <number of messages>] " +
                             "[-size <size of each message (in bytes)>] [-host <host>] [-port <port>]");
    }
}
