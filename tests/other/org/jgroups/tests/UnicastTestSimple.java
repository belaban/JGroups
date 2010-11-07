
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;


/**
 * Tests {@link org.jgroups.protocols.UNICAST or {@link org.jgroups.tests.UNICAST2_ConnectionTests
 * by sending unicast messages between a sender and a receiver. Which of the unicast protocols is tested depends on
 * the stack config passed to the program
 *
 * @author Bela Ban
 * @version $Id: UnicastTestSimple.java,v 1.3 2010/03/12 14:48:32 belaban Exp $
 */
public class UnicastTestSimple extends ReceiverAdapter {
    private JChannel channel;
    private final MyReceiver receiver=new MyReceiver();
    static final String groupname="UnicastTestSimpleGroup";
    private boolean oob=false;


    public void init(String props, String name) throws Exception {
        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        channel.connect(groupname);
        channel.setReceiver(receiver);

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }
    }


    public void eventLoop() throws Exception {
        int c;
        int count=1;

        while(true) {
            System.out.print("[1] Send msgs [2] Print view [3] Print conns [4] Trash conn [5] Trash all conns" +
                    "\n[o] Toggle OOB (" + oob + ")\n[q] Quit\n");
            System.out.flush();
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                sendMessage(count++);
                break;
            case '2':
                printView();
                break;
            case '3':
                printConnections();
                break;
            case '4':
                removeConnection();
                break;
            case '5':
                removeAllConnections();
                break;
            case 'o':
                oob=!oob;
                System.out.println("oob=" + oob);
                break;
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }

    private void printConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(UNICAST.class, UNICAST2.class);
        if(prot instanceof UNICAST)
            System.out.println(((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println(((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            Protocol prot=channel.getProtocolStack().findProtocol(UNICAST.class, UNICAST2.class);
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(UNICAST.class, UNICAST2.class);
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
    }


    void sendMessage(int val) throws Exception {
        Address destination=getReceiver();
        if(destination == null) {
            System.err.println("UnicastTest.sendMessages(): receiver is null, cannot send messages");
            return;
        }


        String str="hello-" + val;
        Message msg=new Message(destination, null, str);
        if(oob)
            msg.setFlag(Message.OOB);
        System.out.println("sending " + str + " to " + destination);
        channel.send(msg);
    }


    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }



    private Address getReceiver() {
        Vector mbrs=null;
        int index;
        BufferedReader reader;
        String tmp;

        try {
            mbrs=channel.getView().getMembers();
            System.out.println("pick receiver from the following members:");
            for(int i=0; i < mbrs.size(); i++) {
                if(mbrs.elementAt(i).equals(channel.getAddress()))
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
            System.err.println("UnicastTest.getReceiver(): " + e);
            return null;
        }
    }


    public static void main(String[] args) {
        String props=null;
        String name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }


        try {
            UnicastTestSimple test=new UnicastTestSimple();
            test.init(props, name);
            test.eventLoop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }

    static void help() {
        System.out.println("UnicastTest [-help] [-props <props>] [-sleep <time in ms between msg sends] " +
                           "[-exit_on_end] [-busy-sleep] [-name name]");
    }




    private static class MyReceiver extends ReceiverAdapter {

        public void receive(Message msg) {
            System.out.println("msg from " + msg.getSrc() + ": " + msg.getObject());
        }

        public void viewAccepted(View new_view) {
            System.out.println("** view: " + new_view);
        }
    }

}