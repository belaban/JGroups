package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Chat implements Receiver {
    protected JChannel channel;
    protected static final String CLUSTER="chat";

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void receive(Message msg) {
        String line="[" + msg.getSrc() + "]: " + msg.getObject();
        System.out.println(line);
    }

    /** Method called from other app, injecting channel */
    public void start(JChannel ch) throws Exception {
        channel=ch;
        channel.setReceiver(this);
        channel.connect(CLUSTER);
        eventLoop();
        channel.close();
    }

    private void start(String props, String name, boolean nohup) throws Exception {
        try {
            channel=new JChannel(props).name(name);
            channel.setReceiver(this);
            channel.connect(CLUSTER);
        }
        catch(Exception ex) {
            Util.close(channel);
            throw ex;
        }

        if(!nohup) {
            eventLoop();
            channel.close();
        }
    }


    private void eventLoop() {
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("> "); System.out.flush();
                String line=in.readLine();
                line=line != null? line.toLowerCase() : null;
                if(line == null)
                    continue;
                if(line.startsWith("quit") || line.startsWith("exit"))
                    break;
                Message msg=new ObjectMessage(null, line);
                channel.send(msg);
            }
            catch(IOException | IllegalArgumentException io_ex) {
                break;
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String  props="udp.xml";
        String  name=null;
        boolean nohup=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-nohup")) {
                nohup=true;
                continue;
            }
            help();
            return;
        }

        Chat ch=new Chat();
        ch.start(props, name, nohup);
    }

    protected static void help() {
        System.out.println("Chat [-props XML config] [-name name] [-nohup]");
    }


}
