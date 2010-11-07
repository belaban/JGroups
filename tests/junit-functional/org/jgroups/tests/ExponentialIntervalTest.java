package org.jgroups.tests;

import org.jgroups.stack.ExponentialInterval;
import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * @author Bela Ban
 * @version $Id: ExponentialIntervalTest.java,v 1.4 2008/04/08 08:29:39 belaban Exp $
 */
public class ExponentialIntervalTest {

    @Test(groups=Global.FUNCTIONAL)
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

    @Test(groups=Global.FUNCTIONAL)
    public static void testNoargConstructor() {
        ExponentialInterval interval=new ExponentialInterval();
        Assert.assertEquals(30, interval.next());
        Assert.assertEquals(60, interval.next());
    }


    @Test(groups=Global.FUNCTIONAL)
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