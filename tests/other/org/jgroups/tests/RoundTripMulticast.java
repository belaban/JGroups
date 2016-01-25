package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.ReceiverAdapter;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

/**
 * Class that measure RTT between a client and server using multicast sockets
 * @author Bela Ban
 */
public class RoundTripMulticast extends ReceiverAdapter {
    MulticastSocket mcast_recv_sock;  // to receive mcast traffic
    MulticastSocket mcast_send_sock;  // to send mcast traffic
    DatagramSocket  ucast_sock;       // to receive and send unicast traffic
    InetAddress bind_addr, mcast_addr;
    int mcast_port=7500;
    int num=1000;
    int msg_size=10;
    boolean server=false;
    final byte[] RSP_BUF={1}; // 1=response
    int   num_responses=0;
    final Object mutex=new Object();
    IpAddress local_addr;


    interface Receiver {
        void receive(byte[] buffer, int offset, int length, InetAddress sender, int sender_port);
    }


    private void start(boolean server, int num, int msg_size, InetAddress bind_addr,
                       InetAddress mcast_addr, int mcast_port) throws Exception {
        this.server=server;
        this.num=num;
        this.msg_size=msg_size;
        this.bind_addr=bind_addr;
        this.mcast_addr=mcast_addr;
        this.mcast_port=mcast_port;

        // SocketAddress saddr=new InetSocketAddress(bind_addr, mcast_port);
        mcast_send_sock=new MulticastSocket(mcast_port);
        mcast_send_sock.setTimeToLive(2);
        mcast_send_sock.setInterface(bind_addr);
        SocketAddress group=new InetSocketAddress(mcast_addr, mcast_port);
        mcast_send_sock.joinGroup(group, null);

        mcast_recv_sock=new MulticastSocket(mcast_port);
        mcast_recv_sock.setTimeToLive(2);
        mcast_recv_sock.setInterface(bind_addr);
        mcast_recv_sock.joinGroup(group, null);

        ucast_sock=new DatagramSocket(0, bind_addr);
        ucast_sock.setTrafficClass(16); // 0x10
        local_addr=new IpAddress(ucast_sock.getLocalAddress(), ucast_sock.getLocalPort());


        if(server) {
            Receiver r=(buf, offset, length, sender, sender_port) -> {
                ByteBuffer buffer=ByteBuffer.wrap(buf, offset, length);
                short len=buffer.getShort();
                byte[] tmp=new byte[len];
                buffer.get(tmp, 0, len);
                try {
                    IpAddress real_sender=(IpAddress)Util.streamableFromByteBuffer(IpAddress.class, tmp);
                    DatagramPacket packet=new DatagramPacket(RSP_BUF, 0, RSP_BUF.length, real_sender.getIpAddress(), real_sender.getPort());
                    ucast_sock.send(packet); // send the response via DatagramSocket
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            };
            ReceiverThread rt=new ReceiverThread(r, mcast_recv_sock);
            rt.start();

            System.out.println("server started (ctrl-c to kill)");
            while(true) {
                Util.sleep(60000);
            }
        }
        else {
            System.out.println("sending " + num + " requests");
            sendRequests();
        }

        mcast_recv_sock.close();
        mcast_send_sock.close();
        ucast_sock.close();
    }



    private void sendRequests() throws Exception {
        byte[]  marshalled_addr=Util.streamableToByteBuffer(local_addr);
        int length=Global.BYTE_SIZE + // request or response byte
                Global.SHORT_SIZE + // length of marshalled IpAddress
                marshalled_addr.length +
                msg_size;
        long    start, stop, total;
        double  requests_per_sec;
        double  ms_per_req;
        int     print=num / 10;
        int     count=0;

        num_responses=0;

        ByteBuffer buffer=ByteBuffer.allocate(length);
        buffer.put((byte)0); // request
        buffer.putShort((short)marshalled_addr.length);
        buffer.put(marshalled_addr, 0, marshalled_addr.length);
        byte[] payload=new byte[msg_size];
        buffer.put(payload, 0, payload.length);
        byte[] array=buffer.array();

        ReceiverThread mcast_receiver=new ReceiverThread(
          (buffer1, offset, length1, sender, sender_port) -> {
              // System.out.println("mcast from " + sender + ":" + sender_port + " was discarded");
          },
                mcast_recv_sock
        );
        mcast_receiver.start();

        ReceiverThread ucast_receiver=new ReceiverThread(
          (buffer1, offset, length1, sender, sender_port) -> {
              synchronized(mutex) {
                  num_responses++;
                  mutex.notify();
              }
          }, ucast_sock);
        ucast_receiver.start();

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            DatagramPacket packet=new DatagramPacket(array, 0, array.length, mcast_addr, mcast_port);
            try {
                mcast_send_sock.send(packet);
                synchronized(mutex) {
                    while(num_responses != count +1) {
                        mutex.wait(1000);
                    }
                    count=num_responses;
                    if(num_responses >= num) {
                        System.out.println("received all responses (" + num_responses + ")");
                        break;
                    }
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


        /*start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            DatagramPacket packet=new DatagramPacket(array, 0, array.length, mcast_addr, mcast_port);
            try {
                mcast_send_sock.send(packet);

                if(num_responses % print == 0) {
                    System.out.println("- received " + num_responses);
                }
                synchronized(mutex) {
                    if(num_responses >= num) {
                        System.out.println("received all responses (" + num_responses + ")");
                        break;
                    }
                    else
                        mutex.wait();
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        stop=System.currentTimeMillis();*/
        total=stop-start;
        requests_per_sec=num / (total / 1000.0);
        ms_per_req=total / (double)num;
        System.out.println("Took " + total + "ms for " + num + " requests: " + requests_per_sec +
                " requests/sec, " + ms_per_req + " ms/request");
    }


    static class ReceiverThread implements Runnable {
        Receiver receiver;
        Thread thread;
        DatagramSocket sock;
        byte[] buf=new byte[65000];
        DatagramPacket packet;

        public ReceiverThread(Receiver r, DatagramSocket sock) {
            this.receiver=r;
            this.sock=sock;
        }

        public final void start() {
            thread=new Thread(this);
            thread.start();
        }

        public void stop() {
            thread=null;
            sock.close();
        }

        public void run() {
            while(thread != null && thread.equals(Thread.currentThread())) {
                packet=new DatagramPacket(buf, 0, buf.length);
                try {
                    sock.receive(packet);
                    if(receiver != null) {
                        receiver.receive(packet.getData(), packet.getOffset(), packet.getLength(), packet.getAddress(), packet.getPort());
                    }
                }
                catch(IOException e) {
                    break;
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        boolean server=false;
        int num=100;
        int msg_size=10; // 10 bytes
        InetAddress bind_addr=null, mcast_addr=null;
        int mcast_port=7500;

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
            if(args[i].equals("-bind_addr")) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-mcast_addr")) {
                mcast_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-mcast_port")) {
                mcast_port=Integer.parseInt(args[++i]);
                continue;
            }
            RoundTripMulticast.help();
            return;
        }

        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();
        if(mcast_addr == null)
            mcast_addr=InetAddress.getByName("225.5.5.5");
        new RoundTripMulticast().start(server, num, msg_size, bind_addr, mcast_addr, mcast_port);
    }



    private static void help() {
        System.out.println("RoundTrip [-server] [-num <number of messages>] " +
                "[-size <size of each message (in bytes)>] [-bind_addr <bind address>] " +
                "[-mcast_addr <mcast addr>] [-mcast_port <mcast port>]");
    }
}
