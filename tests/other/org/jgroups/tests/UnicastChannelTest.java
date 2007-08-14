// $Id: UnicastChannelTest.java,v 1.6 2007/08/14 15:33:36 belaban Exp $


package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;


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



        if(server) {
            runServer();
        }
        else {
            runClient();
        }

    }

    void runClient() throws Exception {
        IpAddress       addr;
        Message         msg;
        Object          obj;
        String          line;
        BufferedReader  reader;

        ch=new JChannel(props);
        ch.connect(null); // unicast channel

        addr=new IpAddress(host, port);
        ch.down(new Event(Event.ENABLE_UNICASTS_TO, addr));
        reader= new BufferedReader(new InputStreamReader(System.in));

        while(true) {
            System.out.print("> ");
            line=reader.readLine();
            if(line.startsWith("quit") || line.startsWith("exit")) {
                ch.close();
                return;
            }
            msg=new Message(addr, null, line);
            ch.send(msg);
            while((obj=ch.peek(1000)) != null) {
                obj=ch.receive(1000);
                if(obj == null)
                    break;
                if(!(obj instanceof Message)) {
                    System.out.println("<-- " + obj);
                }
                else {
                    System.out.println("<-- " + ((Message)obj).getObject());
                }
            }
        }
    }

    void runServer() throws Exception {
        Object  obj;
        Message msg, rsp;

        ch=new JChannel(props);
        System.setProperty("jgroups.bind_addr", host);
        if(port > 0) {
            // ugly hack
            TP transport=(TP)ch.getProtocolStack().findProtocol("UDP");
            if(transport == null)
                transport=(TP)ch.getProtocolStack().findProtocol("TCP");
            if(transport == null)
                transport=(TP)ch.getProtocolStack().findProtocol("TCP_NIO");
            if(transport != null) {
                Properties tmp=new Properties();
                tmp.setProperty("bind_port", String.valueOf(port));
                transport.setProperties(tmp);
            }
        }
        ch.connect(null); // this makes it a unicast channel
        System.out.println("server started at " + new java.util.Date() + ", listening on " + ch.getLocalAddress());
        while(true) {
            obj=ch.receive(0);
            if(!(obj instanceof Message)) {
                System.out.println(obj);
            }
            else {
                msg=(Message)obj;
                System.out.println("-- " + msg.getObject());
                Address sender=msg.getSrc();
                rsp=new Message(sender, null, "ack for " + msg.getObject());
                ch.down(new Event(Event.ENABLE_UNICASTS_TO, sender));
                ch.send(rsp);
            }
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
