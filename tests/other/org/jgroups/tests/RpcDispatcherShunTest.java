package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.io.IOException;
import java.util.Vector;

/**
 * Tests message sending and shunning.Use:
 * <ol>
 * <li>Start multiple instances
 * <li>Send a few messages
 * <li>Shun a member
 * <li>Send a message from another member (must work !)
 * </ol>
 * @author Bela Ban
 * @version $Id: RpcDispatcherShunTest.java,v 1.1 2005/01/10 11:25:41 belaban Exp $
 */
public class RpcDispatcherShunTest implements MembershipListener, RequestHandler {
    JChannel           channel;
    RpcDispatcher      disp;


    public static void main(String[] args) {
        String props=null;
        for(int i=0; i < args.length; i++) {
            String arg=args[i];
            if(arg.equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }
        try {
            new RpcDispatcherShunTest().start(props);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void start(String props) throws IOException, ChannelException {
        channel=new JChannel(props);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        disp=new RpcDispatcher(channel, null, this, this,
                false, // deadlock detection is disabled
                true); // concurrent processing is enabled
        channel.connect("MessageDispatcherTestGroup");
        mainLoop();
    }

    private void mainLoop() throws IOException {
        while(true) {
            int c;
            System.in.skip(System.in.available());
            System.out.println("\n[1] Send [2] Shun [3] Print view [q] Quit");
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                invokeGroupMethod();
                break;
            case '2':
                shun();
                break;
            case '3':
                View v=channel.getView();
                System.out.println("View: " + v);
                break;
            case 'q':
                channel.close();
                return;
            default:
                break;
            }
        }
    }

    public Object helloWorld() {
        System.out.println("method helloWorld() was called");
        return "same to you";
    }

    private void shun() {
        System.out.println("shunning this member");
        channel.up(new Event(Event.EXIT));
    }

    private void invokeGroupMethod() {
        RspList rsp_list;
        View v=channel.getView();
        Vector members=new Vector(v.getMembers());
        System.out.println("sending to " + members);
        rsp_list=disp.callRemoteMethods(members, "helloWorld", null, (String[])null, GroupRequest.GET_ALL, 0);
        System.out.println("responses:\n" + rsp_list);
    }

    private static void help() {
        System.out.println("MessageDispatcherShunTest [-help] [-props <props>]");
    }

    public Object handle(Message msg) {
        return "same to you";
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- view: " +  new_view);
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }
}
