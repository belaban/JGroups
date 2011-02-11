package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

/**
 * Interactive test for MFC and UFC
 * @author Bela Ban
 */
public class FlowControlTest extends ReceiverAdapter {
    protected JChannel ch;
    protected byte[] buf=new byte[50000];

    public void start(String props, String name) throws Exception {
        ch=new JChannel(props);
        ch.setName(name);
        ch.setReceiver(this);
        ch.connect("FlowControlTest");
        loop();
        Util.close(ch);
    }

    public void receive(Message msg) {
        System.out.println("<< " + msg.getLength() + " bytes from " + msg.getSrc());
    }

    public void viewAccepted(View view) {
        System.out.println("view = " + view);
    }


    protected void loop() {
        for(;;) {
            int key=Util.keyPress("[1] send multicast message [2] send unicast message [3] set message size [q] quit");
            switch(key) {
                case '1':
                    Message msg=new Message(null, null, buf);
                    try {
                        ch.send(msg);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case '2':
                    Address target=getReceiver();
                    msg=new Message(target, null, buf);
                    try {
                        ch.send(msg);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case '3':
                    try {
                        int msg_size=Util.readIntFromStdin("New message size: ");
                        buf=new byte[msg_size];
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case 'q': case 'Q': case 'x':
                    return;
            }
        }
    }


    private Address getReceiver() {
        Vector mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=ch.getView().getMembers();
            System.out.println("pick the target from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(ch.getAddress()))
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i) + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbrs.elementAt(i));
            }
            System.out.flush();
            System.in.skip(System.in.available());
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            index=Integer.parseInt(tmp);
            return (Address)mbrs.elementAt(index); // index out of bounds caught below
        }
        catch(Exception e) {
            System.err.println("getReceiver(): " + e);
            return null;
        }
    }


    public static void main(String[] args) throws Exception {
        String name=null;
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("FlowControlTest [-props <properties>] [-name <name>]");
            return;
        }

        FlowControlTest test=new FlowControlTest();
        test.start(props, name);
    }
}
