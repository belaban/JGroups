package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.AckReceiverWindow;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.6 2009/04/27 11:28:08 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class AckReceiverWindowTest {


    public static void test1() {
        Message m;
        AckReceiverWindow win=new AckReceiverWindow(10);
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


    public static void testDuplicates() {
        boolean rc;
        AckReceiverWindow win=new AckReceiverWindow(2);
        Assert.assertEquals(0, win.size());
        rc=win.add(9, msg());
        assert rc;
        rc=win.add(1, msg());
        assert !(rc);
        rc=win.add(2, msg());
        assert rc;
        rc=win.add(2, msg());
        assert !(rc);
        rc=win.add(0, msg());
        assert !(rc);
        rc=win.add(3, msg());
        assert rc;
        rc=win.add(4, msg());
        assert rc;
        rc=win.add(4, msg());
        assert !(rc);
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


    public static void testRemoveOOBMessage() {
        AckReceiverWindow win=new AckReceiverWindow(1);
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
