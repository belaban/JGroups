package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckReceiverWindow;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.1.4.1 2008/06/04 14:16:14 belaban Exp $
 */
public class AckReceiverWindowTest extends TestCase {
    AckReceiverWindow win;


    public AckReceiverWindowTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void test1() {
        Message m;
        win=new AckReceiverWindow(10);
        assertEquals(0, win.size());
        win.add(9, msg());
        assertEquals(0, win.size());

        win.add(10, msg());
        assertEquals(1, win.size());

        win.add(13, msg());
        assertEquals(2, win.size());

        m=win.remove();
        assertNotNull(m);
        assertEquals(1, win.size());

        m=win.remove();
        assertNull(m);
        assertEquals(1, win.size());

        win.add(11, msg());
        win.add(12, msg());
        assertEquals(3, win.size());

        m=win.remove();
        assertNotNull(m);
        m=win.remove();
        assertNotNull(m);
        m=win.remove();
        assertNotNull(m);
        assertEquals(0, win.size());
        m=win.remove();
        assertNull(m);
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
        win=new AckReceiverWindow(1);
        System.out.println("win = " + win);
        assert !win.hasMessagesReadyToRemove();
        win.add(2, msg());
        System.out.println("win = " + win);
        assert !win.hasMessagesReadyToRemove();
        win.add(3, msg());
        System.out.println("win = " + win);
        assert !win.hasMessagesReadyToRemove();
        win.add(1, msg());
        System.out.println("win = " + win);
        assert win.hasMessagesReadyToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert win.hasMessagesReadyToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert win.hasMessagesReadyToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert win.hasMessagesReadyToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert !win.hasMessagesReadyToRemove();
    }

    private static Message msg() {
        return new Message();
    }


    public static Test suite() {
        return new TestSuite(AckReceiverWindowTest.class);
    }


    public static void main(String[] args) {
        String[] testCaseName={AckReceiverWindowTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
