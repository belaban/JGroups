package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.util.Util;

/**
 * Tests the "NAKACK retransmit message lost" problem. Start 2 members, then a third one, and you should never see
 * the problem with messages than cannot be retransmitted because they were already garbage-collected.
 * @author Bela Ban Apr 4, 2004
 * @version $Id: NAKACKTest.java,v 1.2 2004/04/08 04:59:04 belaban Exp $
 */
public class NAKACKTest {
    Channel ch;
    Address local_addr;
    Receiver receiver;
    RpcDispatcher disp;

    class Receiver extends Thread {
        public void run() {
            Object obj;
            Message msg;
            boolean running=true;
            while(running) {
                try {
                    obj=ch.receive(0);
                    if(obj instanceof Message) {
                        msg=(Message)obj;
                        System.out.println(msg.getSrc() + "::" + msg.getObject());
                    }
                    else
                        System.out.println("received " + obj);
                }
                catch(ChannelNotConnectedException e) {
                    running=false;
                }
                catch(ChannelClosedException e) {
                    running=false;
                }
                catch(TimeoutException e) {
                    ;
                }
            }
        }
    }

    public void receive(Address sender, Long i) {
        System.out.println(sender + "::" + i);
    }

    void start(String props, boolean use_rpc) throws Exception {
        long i=0;
        Message msg;
        ch=new JChannel(props);
        if(use_rpc)
           disp=new RpcDispatcher(ch, null, null, this);
        ch.connect("NAKACKTest");
        local_addr=ch.getLocalAddress();
        if(use_rpc == false) {
            receiver=new Receiver();
            receiver.start();
        }
        while(true) {
            if(use_rpc) {
                disp.callRemoteMethods(null, "receive", new Object[]{local_addr, new Long(i++)},
                        new Class[]{Address.class, Long.class}, GroupRequest.GET_ALL, 10000);
            }
            else {
                msg=new Message(null, null, new Long(i++));
                ch.send(msg);
            }
            //Util.sleep(1);
        }
    }

    public static void main(String[] args) {
        String props=null;
        boolean use_rpc=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-use_rpc")) {
                use_rpc=true;
                continue;
            }
            help();
            return;
        }


        try {
            new NAKACKTest().start(props, use_rpc);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void help() {
        System.out.println("NAKACKTest [-help] [-props properties] [-use_rpc]");
    }
}
