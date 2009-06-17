// $Id: MessageDispatcherTestAsync.java,v 1.15 2009/06/17 16:28:59 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RspCollector;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.IOException;


/**
 * Asynchronous example for MessageDispatcher; message is mcast to all members, responses are received
 * asynchronously by calling RspCollector.receiveResponse(). Message is periodically broadcast to all
 * members; handle() method is invoked whenever a message is received.
 *
 * @author Bela Ban
 */
public class MessageDispatcherTestAsync implements RequestHandler {
    Channel channel;
    MessageDispatcher disp;
    RspList rsp_list;
    MyCollector coll=new MyCollector();
    boolean done_submitted=true;
    static final int NUM=10;


    String props="UDP(loopback=true;mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=10000;max_interval=20000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true)";


    static class MyCollector implements RspCollector {

        public void receiveResponse(Object retval, Address sender) {
            System.out.println("** received response " + retval + " [sender=" + sender + ']');
        }

        public void suspect(Address mbr) {
            System.out.println("** suspected member " + mbr);
        }

        public void viewChange(View new_view) {
            System.out.println("** received new view " + new_view);
        }
    }



    public void start() throws Exception {
        channel=new JChannel(props);
        //channel.setOpt(Channel.LOCAL, Boolean.FALSE);
        disp=new MessageDispatcher(channel, null, null, this);
        channel.connect("MessageDispatcherTestAsyncGroup");
    }


    public void mcast(int num) throws IOException {
        if(!done_submitted) {
            System.err.println("Must submit 'done' (press 'd') before mcasting new message");
            return;
        }
        for(int i=0; i < num; i++) {
            Util.sleep(100);
            System.out.println("Casting message #" + i);
            disp.castMessage(null,
                    i,
                    new Message(null, null, "Number #" + i),
                    coll);
        }
        done_submitted=false;
    }


    public void disconnect() {
        System.out.println("** Disconnecting channel");
        channel.disconnect();
        System.out.println("** Disconnecting channel -- done");

        System.out.println("** Closing channel");
        channel.close();
        System.out.println("** Closing channel -- done");

        System.out.println("** disp.stop()");
        disp.stop();
        System.out.println("** disp.stop() -- done");
    }


    public void done() {
        for(int i=0; i < NUM; i++)
            disp.done(i);
        done_submitted=true;
    }


    public Object handle(Message msg) {
        Object tmp=msg.getObject();
        System.out.println("** handle(" + tmp + ')');
        return tmp + ": success";
    }


    public static void main(String[] args) {
        int c;
        MessageDispatcherTestAsync test=null;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
        }



        try {
            test=new MessageDispatcherTestAsync();
            test.start();
            while(true) {
                System.out.println("[m=mcast " + NUM + " msgs x=exit]");
                c=System.in.read();
                switch(c) {
                    case 'x':
                        test.disconnect();
                        System.exit(0);
                        return;
                    case 'm':
                        test.mcast(NUM);
                        break;
                    case 'd':
                        test.done();
                        break;
                    default:
                        break;
                }

            }
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("MessageDispatcherTestAsync");
    }

}
