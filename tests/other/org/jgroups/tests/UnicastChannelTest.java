// $Id: UnicastChannelTest.java,v 1.8.2.3 2009/09/08 12:25:22 belaban Exp $


package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 * Interactive program to test a unicast channel
 * @author Bela Ban March 16 2003
 */
public class UnicastChannelTest {
    boolean  server=false;
    String   host="localhost";
    int      port=0;
    String   props=null;
    JChannel ch;


    public void start(String[] args) throws Exception {

        for(int i=0; i < args.length; i++) {
            String tmp=args[i];

            if("-server".equals(tmp)) {
                server=true;
                continue;
            }

            if("-props".equals(tmp)) {
                props=args[++i];
                continue;
            }

            if("-host".equals(tmp)) {
                host=args[++i];
                continue;
            }

            if("-port".equals(tmp)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }

            help();
            return;
        }

        ch=new JChannel(props);

        if(server) {
            ch.setReceiver(new ReceiverAdapter() {
                public void receive(Message msg) {
                    System.out.println("-- " + msg.getObject());
                    Address sender=msg.getSrc();
                    Message rsp=new Message(sender, null, "ack for " + msg.getObject());
                    try {
                        ch.send(rsp);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            runServer();
        }
        else {
            ch.setReceiver(new ReceiverAdapter() {
                public void receive(Message msg) {
                    System.out.println("<-- " + msg.getObject());
                }

                public void viewAccepted(View new_view) {
                    System.out.println("new_view = " + new_view);
                }

                public void suspect(Address suspected_mbr) {
                    System.out.println("suspected_mbr = " + suspected_mbr);
                }
            });
            runClient();
        }

    }

    void runClient() throws Exception {
        IpAddress       addr;
        Message         msg;
        String          line;
        BufferedReader  reader;

        ch.connect(null); // unicast channel
        addr=new IpAddress(host, port);
        reader=new BufferedReader(new InputStreamReader(System.in));

        while(true) {
            System.out.print("> ");
            line=reader.readLine();
            if(line.startsWith("quit") || line.startsWith("exit")) {
                ch.close();
                return;
            }
            msg=new Message(addr, null, line);
            ch.send(msg);
        }
    }

    void runServer() throws Exception {
        System.setProperty("jgroups.bind_addr", host);
        if(port > 0) {
            Protocol transport=ch.getProtocolStack().getTransport();
            if(transport instanceof TP) {
                ((TP)transport).setBindPort(port);
            }
        }
        ch.connect(null); // this makes it a unicast channel
        System.out.println("server started at " + new java.util.Date() + ", listening on " + ch.getLocalAddress());
        while(ch.isOpen()) {
            Util.sleep(10000);
        }
    }

    static void help() {
        System.out.println("UnicastChannelTest [-help] [-server] [-props <props>]" +
                           "[-host <host>] [-port <port>]");
    }


    public static void main(String[] args) {
        try {
            new UnicastChannelTest().start(args);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

}
