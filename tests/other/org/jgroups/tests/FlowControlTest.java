package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.UFC;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

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
            int key=Util.keyPress("[1] Send multicast message [2] Send unicast message " +
                                    "[3] Set message size [4] Print credits MFC [5] Print credits UFC [q] quit");
            switch(key) {
                case '1':
                    Message msg=new BytesMessage(null, buf);
                    try {
                        ch.send(msg);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case '2':
                    Address target=getReceiver();
                    msg=new BytesMessage(target, buf);
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
                case '4':
                    MFC mfc=ch.getProtocolStack().findProtocol(MFC.class);
                    if(mfc == null) {
                        System.err.println("MFC not found");
                        break;
                    }
                    System.out.println(mfc.printCredits());
                    break;
                case '5':
                    UFC ufc=ch.getProtocolStack().findProtocol(UFC.class);
                    if(ufc == null) {
                        System.err.println("UFC not found");
                        break;
                    }
                    System.out.println(ufc.printCredits());
                    break;
                case 'q': case 'Q': case 'x': case -1:
                    return;
            }
        }
    }


    private Address getReceiver() {
        List<Address> mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=ch.getView().getMembers();
            System.out.println("pick the target from the following members:");
            int i=0;
            for(Address mbr: mbrs) {
                if(mbr.equals(ch.getAddress()))
                    System.out.println("[" + i + "]: " + mbr + " (self)");
                else
                    System.out.println("[" + i + "]: " + mbr);
                i++;
            }
            System.out.flush();
            System.in.skip(System.in.available());
            reader=new BufferedReader(new InputStreamReader(System.in));
            tmp=reader.readLine().trim();
            index=Integer.parseInt(tmp);
            return mbrs.get(index); // index out of bounds caught below
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
