// $Id: UnicastChannelTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $


package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.log.*;

import java.io.*;


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

            if(tmp.equals("-server")) {
                server=true;
                continue;
            }

            if(tmp.equals("-props")) {
                props=args[++i];
                continue;
            }

            if(tmp.equals("-host")) {
                host=args[++i];
                continue;
            }

            if(tmp.equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }

            help();
            return;
        }

        Trace.init();

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
        ch.connect(null); // this makes it a unicast channel
        System.out.println("server started at " + new java.util.Date() +
                           ", listening on " + ch.getLocalAddress());
        while(true) {
            obj=ch.receive(0);
            if(!(obj instanceof Message)) {
                System.out.println(obj);
            }
            else {
                msg=(Message)obj;
                System.out.println("-- " + msg.getObject());
                rsp=new Message(msg.getSrc(), null, "ack for " + msg.getObject());
                ch.send(rsp);
            }
        }
    }

    void help() {
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
