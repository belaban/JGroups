package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckReceiverWindow;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.1.4.6 2009/09/18 07:58:29 belaban Exp $
 */
public class AckReceiverWindowTest extends TestCase {
    AckReceiverWindow win;


    public AckReceiverWindowTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        win=new AckReceiverWindow(1);
    }

    public void tearDown() throws Exception {
        win.reset();
        super.tearDown();
    }

    public void testAdd() {
        win.reset();
        win=new AckReceiverWindow(10);
        Message msg;
        assertEquals(0, win.size());
        win.add(9, msg());
        assertEquals(0, win.size());

        win.add(10, msg());
        assertEquals(1, win.size());

        win.add(13, msg());
        assertEquals(2, win.size());

        msg=win.remove();
        assertNotNull(msg);
        assertEquals(1, win.size());

        msg=win.remove();
        assertNull(msg);
        assertEquals(1, win.size());

        win.add(11, msg());
        win.add(12, msg());
        assertEquals(3, win.size());

        msg=win.remove();
        assertNotNull(msg);
        msg=win.remove();
        assertNotNull(msg);
        msg=win.remove();
        assertNotNull(msg);
        assertEquals(0, win.size());
        msg=win.remove();
        assertNull(msg);
    }

    public void testAddExisting() {
        win.add(1, msg());
        assertEquals(1, win.size());
        win.add(1, msg());
        assertEquals(1, win.size());
        win.add(2, msg());
        assertEquals(2, win.size());
    }

    public void testAddLowerThanNextToRemove() {
        win.add(1, msg());
        win.add(2, msg());
        win.remove(); // next_to_remove is 2
        win.add(1, msg());
        assertEquals(1, win.size());
    }

    public void testRemove() {
        win.add(2, msg());
        Message msg=win.remove();
        assertNull(msg);
        assertEquals(1, win.size());
        win.add(1, msg());
        assertEquals(2, win.size());
        assertNotNull(win.remove());
        assertEquals(1, win.size());
        assertNotNull(win.remove());
        assertEquals(0, win.size());
        assertNull(win.remove());
    }

    public void testRemoveOOBMessage() {
        win.add(2, msg());
        System.out.println("win = " + win);
        assertNull(win.removeOOBMessage());
        assertNull(win.remove());
        win.add(1, msg(true));
        System.out.println("win = " + win);
        assertNotNull(win.removeOOBMessage());
        System.out.println("win = " + win);
        assertNull(win.removeOOBMessage());
        assertNotNull(win.remove());
        assertNull(win.remove());
        assertNull(win.removeOOBMessage());
    }

    public void testDuplicates() {
        boolean rc;
        win=new AckReceiverWindow(2);
        assertEquals(0, win.size());
        rc=win.add(9, msg());
        assertTrue(rc);
        rc=win.add(1, msg());
        assertFalse(rc);
        rc=win.add(2, msg());
        assertTrue(rc);
        rc=win.add(2, msg());
        assertFalse(rc);
        rc=win.add(0, msg());
        assertFalse(rc);
        rc=win.add(3, msg());
        assertTrue(rc);
        rc=win.add(4, msg());
        assertTrue(rc);
        rc=win.add(4, msg());
        assertFalse(rc);
    }

    public void testMessageReadyForRemoval() {
        assertFalse(win.hasMessagesToRemove());
        win.add(2, msg());
        System.out.println("win = " + win);
        assertFalse(win.hasMessagesToRemove());
        win.add(3, msg());
        System.out.println("win = " + win);
        assertFalse(win.hasMessagesToRemove());
        win.add(1, msg());
        System.out.println("win = " + win);
        assertTrue(win.hasMessagesToRemove());

        win.remove();
        System.out.println("win = " + win);
        assertTrue(win.hasMessagesToRemove());

        win.remove();
        System.out.println("win = " + win);
        assertTrue(win.hasMessagesToRemove());

        win.remove();
        System.out.println("win = " + win);
        assertFalse(win.hasMessagesToRemove());
    }



    private static Message msg() {
        return msg(false);
    }

    private static Message msg(boolean oob) {
        Message retval=new Message();
        if(oob)
            retval.setFlag(Message.OOB);
        return retval;
    }

    public static Test suite() {
        return new TestSuite(AckReceiverWindowTest.class);
    }


 

}
