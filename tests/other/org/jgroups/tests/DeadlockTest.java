// $Id: DeadlockTest.java,v 1.8 2008/10/10 14:53:30 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;



/**
 * Class which tests deadlock detection in RequestCorrelator.
 * @author John Giorgiadis
 */
public class DeadlockTest {
    private static final Class[] TYPES=new Class[]{};
    private static final Object[] ARGS=new Object[]{};

    public class InRpc {
        public void rpc_1() { _in_rpc_1(); }
        public void rpc_2() { _in_rpc_2(); }
    }


	private static class Handler implements MessageListener, MembershipListener {
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
	private String stack = null; // default stack config
	private JChannel channel;
	private RpcDispatcher disp;


	private void _in_rpc_1() {
		System.out.println("In rpc_1()");
		cast_call("rpc_2", ARGS, TYPES);
		System.out.println("Exiting rpc_1()");
	}

	private void _in_rpc_2() {
		System.out.println("In rpc_2()");
		System.out.println("Exiting rpc_2()");
	}


	private void cast_call(String method, Object[] args, Class[] types) {
		MethodCall call;
		call = new MethodCall(method, args, types);
		disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0, false, true);
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
		cast_call("rpc_1", ARGS, TYPES);
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
	    new DeadlockTest(Boolean.valueOf(args[0]).booleanValue());
	}
}
