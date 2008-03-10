package org.jgroups.tests;


import org.jgroups.Message;
import org.jgroups.Global;
import org.jgroups.stack.AckReceiverWindow;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Bela Ban
 * @version $Id: AckReceiverWindowTest.java,v 1.2 2008/03/10 15:39:21 belaban Exp $
 */
public class AckReceiverWindowTest {


    @Test(groups=Global.FUNCTIONAL)
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


    @Test(groups=Global.FUNCTIONAL)
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

    private static Message msg() {
        return new Message();
    }




}
