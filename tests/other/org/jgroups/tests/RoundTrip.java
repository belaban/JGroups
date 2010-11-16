package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * Class that measure RTT between a client and server
 * @author Bela Ban
 */
public class RoundTrip extends ReceiverAdapter {
    JChannel channel;
    String props;
    int num=1000;
    int msg_size=10;
    boolean server=false;
    final byte[] RSP_BUF=new byte[]{1}; // 1=response
    int   num_responses=0;
    final Object mutex=new Object(); // to sync sending and reception of a message


    private void start(boolean server, int num, int msg_size, String props) throws ChannelException {
        this.server=server;
        this.num=num;
        this.msg_size=msg_size;
        this.props=props;

        channel=new JChannel(props);
        channel.setReceiver(this);
        channel.connect("rt");

        if(server) {
            System.out.println("server started (ctrl-c to kill)");
            while(true) {
                Util.sleep(60000);
            }
        }
        else {
            channel.setOpt(Channel.LOCAL, Boolean.FALSE);
            System.out.println("sending " + num + " requests");
            sendRequests();
            channel.close();
        }
    }

    /**
     * On the server: receive a request, send a response. On the client: send a request, wait for the response
     * @param msg
     */
    public void receive(Message msg) {
        byte[] buf=msg.getRawBuffer();
        if(buf == null) {
            System.err.println("buffer was null !");
            return;
        }
        if(buf[0] == 0) { // request
            if(!server) {// client ignores requests
                return;
            }
            // System.out.println("-- SERVER: received " + num_requests + " requests");
            Message response=new Message(msg.getSrc(), null, null);
            response.setBuffer(RSP_BUF, 0, RSP_BUF.length);
            try {
                channel.send(response);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        else { // response
            synchronized(mutex) {
                num_responses++;
                // System.out.println("-- SERVER: received " + num_responses + " responses");
                mutex.notify();
            }
        }
    }

    private void sendRequests() {
        byte[] buf=new byte[msg_size];
        long   start, stop, total;
        double requests_per_sec;
        double    ms_per_req;
        Message msg;
        int     print=num / 10;
        int     count=0;

        num_responses=0;
        for(int i=0; i < buf.length; i++) {
            buf[i]=0; // 0=request
        }

/*        Address dest;
        Vector v=new Vector(channel.getView().getMembers());
        v.remove(channel.getLocalAddress());
        dest=(Address)v.firstElement();*/

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            msg=new Message(null, null, null);
            msg.setBuffer(buf);
            try {
                channel.send(msg);
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
        total=stop-start;
        requests_per_sec=num / (total / 1000.0);
        ms_per_req=total / (double)num;
        System.out.println("Took " + total + "ms for " + num + " requests: " + requests_per_sec +
                " requests/sec, " + ms_per_req + " ms/request");
    }


    public static void main(String[] args) throws ChannelException {
        boolean server=false;
        int num=100;
        int msg_size=10; // 10 bytes
        String props=null;

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
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }
        new RoundTrip().start(server, num, msg_size, props);
    }



    private static void help() {
        System.out.println("RoundTrip [-server] [-num <number of messages>] " +
                "[-size <size of each message (in bytes)>] [-props <properties>]");
    }
}
