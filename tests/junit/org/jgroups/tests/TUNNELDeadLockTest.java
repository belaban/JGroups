// $Id: TUNNELDeadLockTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.log.Trace;
import org.jgroups.stack.Router;
import org.jgroups.util.Promise;
import org.jgroups.Message;
import org.jgroups.TimeoutException;

/**
 * Test designed to make sure the TUNNEL doesn't lock the client and the Router
 * under heavy load. 
 * @see TUNNELDeadLockTest#testStress
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @version $Revision: 1.1 $
 **/
public class TUNNELDeadLockTest extends TestCase {

    private JChannel channel;
    private Promise promise;
    private volatile int receivedCnt;

    // the total number of the messages pumped down the channel
    private int msgCount = 30000;
    // the message payload size (in bytes);
    private int payloadSize = 32;
    // the time (in ms) the main thread waits for all the messages to arrive,
    // before declaring the test failed.
    private int mainTimeout = 60000;


    public TUNNELDeadLockTest(String name) {
	super(name);
    }

    public void setUp() throws Exception {
	super.setUp();
	promise = new Promise();
	Trace.setDefaultOutput(Trace.WARN, System.err);
    }

    public void tearDown() throws Exception {

	super.tearDown();

	// I prefer to close down the channel innside the test itself, for the 
	// reason that the channel might be brought in an uncloseable state by 
	// the test. 

	// TO_DO: no elegant way to stop the Router threads and clean-up
	//        resources. Use the Router administrative interface, when
	//        available.
	
	channel = null;
	promise.reset();
	promise = null;
    }


    private String getTUNNELProps(int routerPort) {
	return "TUNNEL(router_host=127.0.0.1;router_port="+routerPort+")";

    }

    /**
     * Pushes messages down the channel as fast as possible. Sometimes this 
     * manages to bring the channel and the Router into deadlock. On the 
     * machine I run it usually happens after 700 - 1000 messages and I 
     * suspect that this number it is related to the socket buffer size.
     * (the comments are written when I didn't solve the bug yet). <br>
     * 
     * The number of messages sent can be controlled with msgCount.
     * The time (in ms) the main threads wait for the all messages to come can
     * be controlled with mainTimeout. If this time passes and the test 
     * doesn't see all the messages, it declares itself failed.
     **/
    public void testStress() throws Exception {

	String props = getTUNNELProps(startRouter());
	channel = new JChannel(props);
 	channel.connect("agroup");

	// receiver thread
	new Thread(new Runnable() {
		public void run() {
		    try {
			while(true) {
                            if(channel == null)
                                return;
			    Object o = channel.receive(10000);
			    if (o instanceof Message) {
				receivedCnt++;
				if (receivedCnt==msgCount) {
				    // let the main thread know I got all msgs
				    promise.setResult(new Object());
				    return;
				}
			    }
			}
		    }
		    catch(TimeoutException e) {
			System.err.println(
                            "Timeout receiving from the channel. "+receivedCnt+
			    " msgs received so far.");
		    }
		    catch(Exception e) {
			System.err.println("Error receiving data");
			e.printStackTrace();
		    }
		}
	    }).start();
	
	// stress send messages - the sender thread
	new Thread(new Runnable() {
		public void run() {
		    try {
			for(int i=0; i<msgCount; i++) {
			    channel.send(null, null, new byte[payloadSize]);
			}
		    }
		    catch(Exception e) {
			System.err.println("Error sending data over ...");
			e.printStackTrace();
		    }
		}
	    }).start();


	// wait for all the messages to come; if I don't see all of them in 
	// mainTimeout ms, I fail the test

	Object result = promise.getResult(mainTimeout);
	if (result==null) {
	    String msg = 
		"The channel has failed to send/receive "+msgCount+" messages "+
		"possibly because of the channel deadlock or too short "+
		"timeout (currently "+mainTimeout+" ms). "+receivedCnt+
		" messages received so far.";
	    fail(msg);
	}

	// don't close it in tearDown() because it hangs forever for a failed 
	// test.
	channel.close();
    }


    public static Test suite() {
	TestSuite s=new TestSuite(TUNNELDeadLockTest.class);
	return s;
    }
    
    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
	System.exit(0);
    }


    //
    // HELPERS
    //

    /**
     * Starts the router on a separate thread and makes sure it answers the 
     * requests. Required by TUNNEL.
     **/
    private int startRouter() throws Exception {

	final int routerPort = getFreePort();
	Thread routerThread = new Thread(new Runnable() {
		public void run() {	
		    try {
			new Router(routerPort).start();
		    }
		    catch(Exception e) {
			System.err.println("Failed to start the router "+
					   "on port "+routerPort);
			e.printStackTrace();
		    }
		}});
	routerThread.start();

	// verify the router - try to connect for 10 secs
	long startms = System.currentTimeMillis();
	long crtms = startms;
	Exception lastConnectException = null;
	while(crtms-startms<10000) {
	    Socket s = null;
	    try {
		s = new Socket("localhost", routerPort);
	    }
	    catch(Exception e) {
		lastConnectException = e;
		crtms = System.currentTimeMillis();
		continue;
	    }
	    lastConnectException = null;
	    DataInputStream dis = new DataInputStream(s.getInputStream());
	    DataOutputStream dos = new DataOutputStream(s.getOutputStream());

	    // read the IpAddress
	    int len = dis.readInt();
	    byte[] buffer = new byte[len];
	    dis.read(buffer,0,len);

	    // write a GET
	    dos.writeInt(Router.GET);
	    dos.writeUTF("nogroup_setup");
	    dis.readInt();
	    
	    s.close();
	    break;
	}
	if (lastConnectException!=null) {
	    lastConnectException.printStackTrace();
	    fail("Cannot connect to the router");
	}
	System.out.println("router ok");
	return routerPort;
    }


    private int getFreePort() throws Exception {
	ServerSocket ss = new ServerSocket(0);
	int port = ss.getLocalPort();
	ss.close();
	return port;
    }

}
