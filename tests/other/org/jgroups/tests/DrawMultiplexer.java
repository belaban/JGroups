package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.util.Vector;

/**
 * Tests cluster method invocations on disconnected and connected services
 * @author Bela Ban
 * @version $Id: DrawMultiplexer.java,v 1.3 2006/07/11 11:51:44 belaban Exp $
 */
public class DrawMultiplexer implements MembershipListener, RequestHandler, ChannelListener {
    Channel           channel;
    JChannelFactory factory=null;
    RpcDispatcher     disp;
    String props=null;
    boolean spurious_channel_created=false;


    public static void main(String[] args) throws Exception {
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
        new DrawMultiplexer().start(props);
    }

    private void start(String props) throws Exception {
        if(factory == null)
            factory=new JChannelFactory();
        if(props == null)
            this.props="stacks.xml";
        else
            this.props=props;
        factory.setMultiplexerConfig(this.props);
        channel=factory.createMultiplexerChannel("fc-fast-minimalthreads", "MyId");

        if(!spurious_channel_created) {
            // create one additional channel so that we don't close the JGroups channel when disconnecting from MuxChannel !
            Channel tmp=factory.createMultiplexerChannel("fc-fast-minimalthreads", "tempChannel");
            tmp.connect("bla");
            spurious_channel_created=true;
        }

        channel.addChannelListener(this);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        disp=new RpcDispatcher(channel, null, this, this,
                               false, // deadlock detection is disabled
                               true); // concurrent processing is enabled
        channel.connect("MessageDispatcherTestGroup");
        mainLoop();
    }



    private void stop() {
        disp.stop();
        channel.close();
    }

    private void mainLoop() throws Exception  {
        while(true) {
            int c;
            System.in.skip(System.in.available());
            System.out.println("\n[1] Send [2] Start service [3] Stop service [4] Print view [q] Quit");
            c=System.in.read();
            switch(c) {
            case -1:
                break;
            case '1':
                invokeGroupMethod();
                break;
            case '2':
                start(this.props);
                break;
            case '3':
                stop();
                break;
            case '4':
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


    private void invokeGroupMethod() {
        RspList rsp_list;
        View v=channel.getView();
        if(v == null)
            return;
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


    public void channelConnected(Channel channel) {
        // System.out.println("-- channel connected");
    }

    public void channelDisconnected(Channel channel) {
        // System.out.println("-- channel disconnected");
    }

    public void channelClosed(Channel channel) {
        // System.out.println("-- channel closed");
    }

    public void channelShunned() {
        // System.out.println("-- channel shunned");
    }

    public void channelReconnected(Address addr) {
        // System.out.println("-- channel reconnected");
    }
}
