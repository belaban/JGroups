// $Id: RpcDispatcherTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.blocks.*;



/**
 * Example for RpcDispatcher (see also MessageDispatcher). A remote method (print()) is group-invoked
 * periodically. The method is defined in each instance and is invoked whenever a remote method call
 * is received. The callee (although in this example, each callee is also a caller (peer principle))
 * has to define the public methods, and the caller uses one of the callRemoteMethods() methods to
 * invoke a remote method. CallRemoteMethods uses the core reflection API to lookup and dispatch
 * methods.
 * @author Bela Ban
 */
public class RpcDispatcherTest {
    Channel            channel;
    RpcDispatcher      disp;
    RspList            rsp_list;
    String             props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:QUEUE";
    





    public int print(int number) throws Exception {
	System.out.println("print(" + number + ")");
	return number * 2;
    }


    public void start() throws Exception {
	channel=new JChannel(props);
	disp=new RpcDispatcher(channel, null, null, this);
	channel.connect("RpcDispatcherTestGroup");

	for(int i=0; i < 10; i++) {
	    Util.sleep(100);
	    rsp_list=disp.callRemoteMethods(null, "print", new Integer(i), GroupRequest.GET_ALL, 0);
	    System.out.println("Responses: " +rsp_list);
	}
	System.out.println("Closing channel");
	channel.close();
	System.out.println("Closing channel: -- done");

	System.out.println("Stopping dispatcher");
	disp.stop();
	System.out.println("Stopping dispatcher: -- done");
    }





    public static void main(String[] args) {
	try {
	    new RpcDispatcherTest().start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }
}
