// $Id: MessageDispatcherTest.java,v 1.2 2003/09/24 23:26:14 belaban Exp $

package org.jgroups.tests;



import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.util.*;




/**
 * Example for MessageDispatcher (see also RpcDispatcherTest). Message is periodically broadcast to all
 * members; handle() method is invoked whenever a message is received.
 */
public class MessageDispatcherTest implements RequestHandler {
    Channel            channel;
    MessageDispatcher  disp;
    RspList            rsp_list;

    String             props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:"+
                         "GMS:VIEW_ENFORCER:QUEUE";

    //String   props="UDP:PING(num_initial_members=2;timeout=3000):FD:" +
    //"pbcast.PBCAST(gossip_interval=5000;gc_lag=30):UNICAST:FRAG:" +
    //"pbcast.GMS:STATE_TRANSFER:QUEUE";



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
	    System.out.println("Responses:\n" +rsp_list);
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
