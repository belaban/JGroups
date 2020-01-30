package org.jgroups.demos;

import org.jgroups.*;

import java.io.BufferedReader;
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
        channel=new JChannel(props);
        if(name != null)
            channel.name(name);
        channel.setReceiver(this);
        channel.connect(CLUSTER);
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
                String line=in.readLine().toLowerCase();
                if(line.startsWith("quit") || line.startsWith("exit")) {
                    break;
                }
                Message msg=new ObjectMessage(null, line);
                channel.send(msg);
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

        new Chat().start(props, name, nohup);
    }

    protected static void help() {
        System.out.println("Chat [-props XML config] [-name name] [-nohup]");
    }
}
