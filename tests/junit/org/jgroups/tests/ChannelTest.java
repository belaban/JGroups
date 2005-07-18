// $Id: ChannelTest.java,v 1.1 2005/07/18 08:49:16 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.tests.*;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.1 2005/07/18 08:49:16 belaban Exp $
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


    public static Test suite() {
        TestSuite s=new TestSuite(ChannelTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }




}


