// $Id: MessageDispatcherTest.java,v 1.9 2004/07/05 06:10:44 belaban Exp $

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
        // disp=new MessageDispatcher(channel, null, null, this);
        disp=new MessageDispatcher(channel, null, null, this,
                false, // deadlock detection is disabled
                true); // concurrent processing is enabled
        channel.connect("MessageDispatcherTestGroup");

//        for(int i=0; i < 10; i++) {
//            //Util.sleep(1000);
//            System.out.println("Casting message #" +i);
//            rsp_list=disp.castMessage(null,
//                    new Message(null, null, new String("Number #" +i).getBytes()),
//                    GroupRequest.GET_ALL, 0);
//            System.out.println("Responses:\n" + rsp_list);
//        }

        MyThread t1=new MyThread("one"), t2=new MyThread("two");
        t1.start();
        t2.start();
        t1.join();
        t2.join();

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


    class MyThread extends Thread {
        public MyThread(String name) {
            setName(name);
        }

        public void run() {
            for(int i=0; i < 10; i++) {
                System.out.println('[' + getName() + "] casting message #" +i);
                rsp_list=disp.castMessage(null,
                        new Message(null, null, '[' + getName() + "] number #" + i),
                        GroupRequest.GET_ALL, 0);
                System.out.println('[' + getName() + "] responses:\n" + rsp_list);
            }
        }
    }


    public Object handle(Message msg) {
        System.out.println("handle(): " + msg.getObject());
        Util.sleepRandom(5000);
        return "Success !";
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
