// $Id: InexistentRouterTest.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.log.Trace;


/**
 * The test was relevant for version 2.0.5. Since 2.0.6, the way JChannel
 * handles connect() changed, and the test stays here for historical reasons
 * only.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @version $Revision: 1.1.1.1 $
 **/
public class InexistentRouterTest extends TestCase {
    private int SERVER_SOCKET_TIMEOUT = 20000;

    private int port;
    private ServerSocket ss;
    private JChannel channel;
    private String action;

    public InexistentRouterTest(String name) {
	super(name);
    }

    public void setUp() throws Exception {

	super.setUp();

	Trace.setDefaultOutput(Trace.WARN, System.err);

	// find an unused port
	ss = new ServerSocket(0);
	port = ss.getLocalPort();
	ss.close();

	debug("ROUTER PORT: "+port);
 	String props = "TUNNEL(router_host=127.0.0.1;router_port="+port+")";
 	channel = new JChannel(props);

    }

    public void tearDown() throws Exception {

	super.tearDown();
	ss.close();
        channel.close();
    }

    
    /**
     * The channel is configured with a router port on which nobody is 
     * listening. The test will connect the channel, will wait for the connect
     * to fail and will start a server socket on the port the router was
     * supposed to listen on. If the server socket gets a connection attempt,
     * the test fails - that means the TUNNEL receiver thread is still trying
     * to reconnect the RouterStub. If the server socket doesn't receive 
     * anything in 20 secs (RouterStub.RECONNECT_TIMEOUT is 5 secs by default)
     * the test is considered successful.
     **/
    public void testReconnect() throws Exception {
	
	try {
	    channel.connect("GROUP");
	}
	catch(ChannelException e) {
	}

	startServerSocket();

    }


    private void startServerSocket() {

	try {
	    debug("Starting server on "+port);
	    ss = new ServerSocket(port);
	    ss.setSoTimeout(SERVER_SOCKET_TIMEOUT);
	    Socket s = ss.accept();
	    fail("The Server Socket received a connection attempt.");
	}
	catch(InterruptedIOException e) {
	    // timeout, no connection - ok
	}
	catch(IOException e) {
	    debug("Socket exception:",e);
	    fail("Not suposed to get exception here.");
	}
    }

    
    private void debug(String msg) {
        System.out.println(msg);
    }

    private void debug(String msg, Throwable t) {
        System.out.println(msg + ", exception=" + t);
    }



    public static Test suite() {
	TestSuite s=new TestSuite(InexistentRouterTest.class);
	return s;
    }
    
    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }


}
