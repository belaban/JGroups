// $Id: InterruptTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import java.net.*;
import junit.framework.*;



/**
 * Tests Thread.interrupt() against InputStream.read(), Object.wait() and Thread.sleep()
 * @author Bela Ban Oct 5 2001
 */
public class InterruptTest extends TestCase {
    Interruptible     thread;
    static final long TIMEOUT=3000;
    static final int  SLEEP=1;
    static final int  WAIT=2;
    static final int  READ=3;
    static final int  SOCKET_READ=4;



    public InterruptTest(String name) {
	super(name);
    }


    public void setUp() {
	
    }

    public void tearDown() {
	
    }

    String modeToString(int m) {
	switch(m) {
	case SLEEP:       return "SLEEP";
	case WAIT:        return "WAIT";
	case READ:        return "READ";
	case SOCKET_READ: return "SOCKET_READ";
	default:          return "<unknown>";
	}
    }
    


    /**
     * Starts the Interruptible and interrupts after TIMEOUT milliseconds. Then joins thread
     * (waiting for TIMEOUT msecs). PASS if thread dead, FAIL if still alive
     */
    public void testSleepInterrupt() {
	thread=new SleeperThread(SLEEP);
	runTest(thread);
    }


    public void testWaitInterrupt() {
	thread=new SleeperThread(WAIT);
	runTest(thread);
    }

    public void testSocketReadInterrupt() {
	thread=new SleeperThread(SOCKET_READ);
	runTest(thread);
    }


    public void testReadInterrupt() {
	thread=new SleeperThread(READ);
	runTest(thread);
    }



    void runTest(Interruptible t) {
	System.out.println();
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): starting other thread");
	thread.start();
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): starting other thread -- done");
	
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): sleeping for " + TIMEOUT + " msecs");
	sleep(TIMEOUT);
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): sleeping -- done");
	
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): interrupting other thread");
	thread.interrupt();
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): interrupting other thread -- done");
	
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): joining other thread (timeout=" + TIMEOUT + " msecs");
	thread.join(TIMEOUT);
	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): joining other thread -- done");

	System.out.println("InterruptTest.runTest(" + modeToString(t.getMode()) + "): thread.isAlive()=" + thread.isAlive());
	assertTrue(!thread.isAlive());
    }


    void sleep(long msecs) {
	try {
	    Thread.sleep(msecs);
	}
	catch(Exception ex) {
	    System.err.println("InterruptTest.sleep(): " + ex);
	}
    }



    interface Interruptible {
	void     start();
	void     interrupt();
	void     join(long msecs);
	boolean  isAlive();
	int      getMode();
    }




    
    class SleeperThread implements Runnable, Interruptible {
	Thread         t;
	int            mode;
	DatagramSocket sock=null;

	
	SleeperThread(int mode) {
	    this.mode=mode;
	}


	public int getMode() {return mode;}

	public void start() {
	    t=new Thread(this);
	    t.start();
	}

	public void interrupt() {

	    // Uncommenting the following code would cause the socket to be closed by
	    // sending it some dummy input

	    /*
	    if(mcast_sock != null) {
		try {
		    System.out.println("** interrupt(): sending packet");
		    byte[] b={0};
		    DatagramPacket p=new DatagramPacket(b, b.length, InetAddress.getLocalHost(), 12345);
		    mcast_sock.send(p);
		}
		catch(Exception e) {
		    System.err.println(e);
		}

		System.out.println("** interrupt(): closing socket " + mcast_sock);
		mcast_sock.close();
		System.out.println("** interrupt(): closed socket");
		mcast_sock=null;
	    }
	    */

	    if(t != null)
		t.interrupt();
	}


	public void join(long msecs) {
	    if(t != null && t.isAlive()) {
		try {
		    t.join(msecs);
		}
		catch(Exception ex) {
		    System.err.println("InterruptTest.SleepThread.join(): " + ex);
		}
	    }
	}

	public boolean isAlive() {
	    if(t != null)
		return t.isAlive();
	    return false;
	}
	
	public void run() {
	    switch(mode) {
	    case SLEEP:
		runSleep();
		break;
	    case WAIT:
		runWait();
		break;
	    case READ:
		runRead();
		break;
	    case SOCKET_READ:
		runSocketRead();
		break;
	    default:
		break;
	    }
	}


	void runSleep() {
	    try {
		Thread.sleep(TIMEOUT);
	    }
	    catch(InterruptedException ex) {
		System.err.println("InterruptTest.SleeperThread.runSleep(): " + ex);
	    }	    
	}

	void runWait() {
	    Object mutex=new Object();
	    synchronized(mutex) {
		try {
		    mutex.wait();
		}
		catch(InterruptedException ex) {
		    System.err.println("InterruptTest.SleeperThread.runWait(): " + ex);
		}
	    }
	}

	void runRead() {
	    int c;
	    try {
		c=System.in.read();
	    }
	    catch(Exception ex) {
		System.err.println("InterruptTest.SleeperThread.runRead(): " + ex);
	    }
	}

	void runSocketRead() {
	    byte[]         buf=new byte[2];
	    DatagramPacket packet;

	    try {
		sock=new DatagramSocket(12345, InetAddress.getLocalHost());
		// System.out.println("** mcast_sock=" + mcast_sock.getLocalAddress() + ":" + mcast_sock.getLocalPort());
		packet=new DatagramPacket(buf, buf.length);
		//System.out.println("** receive(): start");
		sock.receive(packet);
		//System.out.println("** receive(): done");
	    }
	    catch(Exception e) {
		//System.out.println("** receive(): done, exception=" + e);
		System.err.println(e);
	    }
	}
    }





    public static Test suite() {
	TestSuite s=new TestSuite(InterruptTest.class);
	return s;
    }
    
    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}


