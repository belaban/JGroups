// $Id: DeadlockTest.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.*;



/**
 * Class which tests deadlock detection in RequestCorrelator.
 * @author John Giorgiadis
 */
public class DeadlockTest {

	public class InRpc {
		public void rpc_1() { _in_rpc_1(); }
		public void rpc_2() { _in_rpc_2(); }
	}


	private class Handler implements MessageListener, MembershipListener {
		public Handler() { super(); }
		// MessageListener
		public byte[] getState() { return(null); }
		public void setState(byte[] state) {}
		public void receive(Message msg) {}
		// MembershipListener
		public void block() {}
		public void suspect(Address suspect) {}
		public void viewAccepted(View view) {}
	}

	// .......................................................................

	private String name = "JG";
	private String stack = "UDP" +
		":PING(num_initial_members=2;timeout=3000)" +
		":FD" +
		":STABLE" +
		":NAKACK" +
		":UNICAST" +
		":FRAG" +
		":FLUSH" +
		":GMS" +
		":VIEW_ENFORCER" +
		":STATE_TRANSFER" +
		":QUEUE";
	private JChannel channel;
	private RpcDispatcher disp;


	private void _in_rpc_1() {
		System.out.println("In rpc_1()");
		cast_call("rpc_2", new Object[]{});
		//channel.disconnect();
		System.out.println("Exiting rpc_1()");
	}

	private void _in_rpc_2() {
		System.out.println("In rpc_2()");
		//channel.disconnect();
		System.out.println("Exiting rpc_2()");
	}


	private void cast_call(String method, Object[] args) {
		MethodCall call;
		call = new MethodCall(method);
		for (int i = 0; i < args.length; ++i) call.addArg(args[i]);
		disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
	}

	// .......................................................................

	public DeadlockTest(boolean use_deadlock_detection) {
		Handler handler = new Handler();
		InRpc in_rpc = new InRpc();

		try {
		    channel = new JChannel(stack);
		    disp = new RpcDispatcher(channel, handler, handler, in_rpc, use_deadlock_detection);
		    channel.connect(name);
		} 
                catch(ChannelClosedException ex) { ex.printStackTrace(); }
                catch(ChannelException ex) { ex.printStackTrace(); }
		
		// Call rpc_1 which in turn calls rpc_2
		System.out.println("Calling rpc_1()");
		if(!use_deadlock_detection)
		    System.out.println("** Not using deadlock detection -- recursive call will hang !");
		else
		    System.out.println("** Using deadlock detection -- recursive call will succeed");
		cast_call("rpc_1", new Object[]{});
		System.out.println("Out of rpc_1()");
		channel.disconnect();
		channel.close();
		System.out.println("Disconnected");
	}

	// .......................................................................

	public static void main(String[] args) {
	    if(args.length != 1) {
		System.out.println("DeadlockTest <true|false (use_deadlock_detection)>");
		return;
	    }
	    new DeadlockTest(new Boolean(args[0]).booleanValue());
	}
}
