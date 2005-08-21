package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckReceiverWindow;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.2 2005/08/21 20:37:50 belaban Exp $
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

    private Message msg() {
        return new Message();
    }


    public static Test suite() {
        TestSuite s=new TestSuite(AckReceiverWindowTest.class);
        return s;
    }


    public static void main(String[] args) {
        String[] testCaseName={AckReceiverWindowTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
