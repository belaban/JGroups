package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.AckReceiverWindow;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.8 2009/09/18 12:43:43 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class AckReceiverWindowTest {
    AckReceiverWindow win;

    @BeforeMethod
    public void setUp() throws Exception {
        win=new AckReceiverWindow(1);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        win.reset();
    }


    public void testAdd() {
        Message m;
        win.reset(); // use a different window for this test method
        win=new AckReceiverWindow(10);
        Assert.assertEquals(0, win.size());
        win.add(9, msg());
        Assert.assertEquals(0, win.size());

        win.add(10, msg());
        Assert.assertEquals(1, win.size());

        win.add(13, msg());
        Assert.assertEquals(2, win.size());

        m=win.remove();
        assert m != null;
        Assert.assertEquals(1, win.size());

        m=win.remove();
        assert m == null;
        Assert.assertEquals(1, win.size());

        win.add(11, msg());
        win.add(12, msg());
        Assert.assertEquals(3, win.size());

        m=win.remove();
        assert m != null;
        m=win.remove();
        assert m != null;
        m=win.remove();
        assert m != null;
        Assert.assertEquals(0, win.size());
        m=win.remove();
        assert m == null;
    }

    public void testAddExisting() {
        win.add(1, msg());
        assert win.size() == 1;
        win.add(1, msg());
        assert win.size() == 1;
        win.add(2, msg());
        assert win.size() == 2;
    }

    public void testAddLowerThanNextToRemove() {
        win.add(1, msg());
        win.add(2, msg());
        win.remove(); // next_to_remove is 2
        win.add(1, msg());
        assert win.size() == 1;
    }

    public void testRemove() {
        win.add(2, msg());
        Message msg=win.remove();
        assert msg == null;
        assert win.size() == 1;
        win.add(1, msg());
        assert win.size() == 2;
        assert win.remove() != null;
        assert win.size() == 1;
        assert win.remove() != null;
        assert win.size() == 0;
        assert win.remove() == null;
    }


    public void testRemoveOOBMessage() {
        System.out.println("win = " + win);
        win.add(2, msg());
        System.out.println("win = " + win);
        assert win.removeOOBMessage() == null;
        assert win.remove() == null;
        win.add(1, msg(true));
        System.out.println("win = " + win);
        assert win.removeOOBMessage() != null;
        System.out.println("win = " + win);
        assert win.removeOOBMessage() == null;
        assert win.remove() != null;
        assert win.remove() == null;
        assert win.removeOOBMessage() == null;
    }

    public void testDuplicates() {
        Assert.assertEquals(0, win.size());
        assert win.add(9, msg());
        assert win.add(1, msg());
        assert win.add(2, msg());
        assert !win.add(2, msg());
        assert !win.add(0, msg());
        assert win.add(3, msg());
        assert win.add(4, msg());
        assert !win.add(4, msg());
    }

    public static void testMessageReadyForRemoval() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        System.out.println("win = " + win);
        assert !win.hasMessagesToRemove();
        win.add(2, msg());
        System.out.println("win = " + win);
        assert !win.hasMessagesToRemove();
        win.add(3, msg());
        System.out.println("win = " + win);
        assert !win.hasMessagesToRemove();
        win.add(1, msg());
        System.out.println("win = " + win);
        assert win.hasMessagesToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert win.hasMessagesToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert win.hasMessagesToRemove();

        win.remove();
        System.out.println("win = " + win);
        assert !win.hasMessagesToRemove();
    }




    public static void testRemoveRegularAndOOBMessages() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        win.add(1, msg());
        System.out.println("win = " + win);
        win.remove();
        System.out.println("win = " + win);
        assert win.size() == 0;

        win.add(3, msg());
        win.remove();
        System.out.println("win = " + win);
        assert win.size() == 1;

        win.add(2, msg(true));
        win.removeOOBMessage();
        System.out.println("win = " + win);

        assert win.size() == 1;
    }

    public static void testSmallerThanNextToRemove() {
        AckReceiverWindow win=new AckReceiverWindow(1);
        Message msg;
        for(long i=1; i <= 5; i++)
            win.add(i, msg());
        System.out.println("win = " + win);
        boolean added=win.add(3, msg());
        assert !added;
        for(;;) {
            msg=win.remove();
            if(msg == null)
                break;
        }
        added=win.add(3, msg());
        assert !added;
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



}
