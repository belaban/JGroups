// $Id: ChannelTest.java,v 1.2 2006/01/11 15:02:34 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.2 2006/01/11 15:02:34 belaban Exp $
 */
public class ChannelTest extends TestCase {
    JChannel ch;

    private static final String GROUP="DiscardTestGroup";


    public ChannelTest(String name) {
        super(name);
    }


    protected void setUp() throws Exception {
        super.setUp();
        ch=new JChannel();
        ch.connect(GROUP);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        ch.close();
    }

    public void testFirstView() throws Exception {
        Object obj=ch.receive(5000);
        if(!(obj instanceof View)) {
            fail("first object returned needs to be a View (was " + obj + ")");
        }
        else {
            System.out.println("view is " + obj);
        }
    }

    public void testReceiveTimeout() throws ChannelException, TimeoutException {
        ch.receive(1000); // this one works, because we're expecting a View

        // .. but this one doesn't (no msg available) - needs to throw a TimeoutException
        try {
            ch.receive(2000);
        }
        catch(ChannelNotConnectedException e) {
            fail("channel should be connected");
        }
        catch(ChannelClosedException e) {
            fail("channel should not be closed");
        }
        catch(TimeoutException e) {
            System.out.println("caught a TimeoutException - this is the expected behavior");
        }
    }

    public void testNullMessage() throws ChannelClosedException, ChannelNotConnectedException {
        try {
            ch.send(null);
            fail("null message should throw an exception - we should not get here");
        }
        catch(NullPointerException e) {
            System.out.println("caught NullPointerException - this is expected");
        }
    }


    public static Test suite() {
        TestSuite s=new TestSuite(ChannelTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }




}


