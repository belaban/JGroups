package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.stack.ExponentialInterval;
import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;


/**
 * @author Bela Ban
 * @version $Id: ExponentialIntervalTest.java,v 1.3 2008/03/10 15:39:21 belaban Exp $
 */
public class ExponentialIntervalTest {

    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public static void testInitialization() {
        ExponentialInterval interval=new ExponentialInterval(10);
        System.out.println("interval=" + interval);
        long value=interval.next();
        System.out.println("interval=" + interval);
        Assert.assertEquals(10, value);
        value=interval.next();
        System.out.println("interval=" + interval);
        Assert.assertEquals(20, value);
    }

    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public static void testNoargConstructor() {
        ExponentialInterval interval=new ExponentialInterval();
        Assert.assertEquals(30, interval.next());
        Assert.assertEquals(60, interval.next());
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public static void testMax() {
        ExponentialInterval interval=new ExponentialInterval(1000);
        System.out.println("interval=" + interval);
        Assert.assertEquals(1000, interval.next());
        System.out.println("interval=" + interval);
        Assert.assertEquals(2000, interval.next());
        System.out.println("interval=" + interval);
        Assert.assertEquals(4000, interval.next());
        System.out.println("interval=" + interval);
        Assert.assertEquals(8000, interval.next());
        System.out.println("interval=" + interval);
        Assert.assertEquals(15000, interval.next());
        System.out.println("interval=" + interval);
        Assert.assertEquals(15000, interval.next());
        System.out.println("interval=" + interval);

    }



}