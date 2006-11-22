// $Id: ChannelTest.java,v 1.3 2006/11/22 19:33:07 vlada Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.TimeoutException;
import org.jgroups.View;


/**
 * Tests various methods in JChannel
 * @author Bela Ban
 * @version $Id: ChannelTest.java,v 1.3 2006/11/22 19:33:07 vlada Exp $
 */
public class ChannelTest extends ChannelTestBase {
    Channel ch;

    private static final String GROUP="DiscardTestGroup";    


    public void setUp() throws Exception {
        super.setUp();
        ch=createChannel();
        ch.connect(GROUP);
    }

    public void tearDown() throws Exception {
        ch.close();
        super.tearDown();        
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


