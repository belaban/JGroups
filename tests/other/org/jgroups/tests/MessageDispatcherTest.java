// $Id: MessageDispatcherTest.java,v 1.3 2003/12/11 06:44:14 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;


/**
 * Example for MessageDispatcher (see also RpcDispatcherTest). Message is periodically broadcast to all
 * members; handle() method is invoked whenever a message is received.
 */
public class MessageDispatcherTest implements RequestHandler {
    Channel channel;
    MessageDispatcher disp;
    RspList rsp_list;
    String props=null;


    public void start() throws Exception {
        channel=new JChannel(props);
        //channel.setOpt(Channel.LOCAL, Boolean.FALSE);
        disp=new MessageDispatcher(channel, null, null, this);
        channel.connect("MessageDispatcherTestGroup");

        for(int i=0; i < 10; i++) {
            Util.sleep(1000);
            System.out.println("Casting message #" + i);
            rsp_list=disp.castMessage(null,
                    new Message(null, null, new String("Number #" + i).getBytes()),
                    GroupRequest.GET_ALL, 0);
            System.out.println("Responses:\n" + rsp_list);
        }
        System.out.println("** Disconnecting channel");
        channel.disconnect();
        System.out.println("** Disconnecting channel -- done");

        System.out.println("** Closing channel");
        channel.close();
        System.out.println("** Closing channel -- done");

        System.out.println("** disp.stop()");
        disp.stop();
        System.out.println("** disp.stop() -- done");

        //Util.printThreads();
        //Util.sleep(2000);
        //Util.printThreads();
    }


    public Object handle(Message msg) {
        System.out.println("handle(): " + msg);
        return new String("Success !");
    }


    public static void main(String[] args) {
        try {
            new MessageDispatcherTest().start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }
}
