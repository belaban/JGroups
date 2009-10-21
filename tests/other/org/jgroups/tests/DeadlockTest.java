// $Id: DeadlockTest.java,v 1.9 2009/10/21 07:51:09 belaban Exp $

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;

import java.lang.reflect.Method;


/**
 * Class which tests deadlocks in RequestCorrelator
 * @author John Giorgiadis
 * @author Bela Ban
 */
public class DeadlockTest {

    public void foo() {
        System.out.println("foo()");
        cast_call(BAR, true);
    }
    public static void bar() {
        System.out.println("bar()");
    }

    static final Method FOO, BAR;

    static {
        try {
            FOO=DeadlockTest.class.getMethod("foo");
            BAR=DeadlockTest.class.getMethod("bar");
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

	// .......................................................................

	private String        name = "DeadlockTest";
	private JChannel      channel;
	private RpcDispatcher disp;



	private void cast_call(Method method, boolean oob) {
		MethodCall call=new MethodCall(method);
        disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0, false, oob);
	}

	// .......................................................................


    public void start() {
		try {
		    channel = new JChannel();
		    disp = new RpcDispatcher(channel, null, null, this);
		    channel.connect(name);
		}
        catch(ChannelClosedException ex) { ex.printStackTrace(); }
        catch(ChannelException ex) { ex.printStackTrace(); }

		// Call foo() which in turn calls bar()
		cast_call(FOO, false);
		channel.close();
		System.out.println("Disconnected");
	}

	// .......................................................................

    public static void main(String[] args) {
        DeadlockTest test=new DeadlockTest();
        test.start();
    }
}
