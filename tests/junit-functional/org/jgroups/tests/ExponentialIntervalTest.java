package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.ExponentialInterval;


/**
 * @author Bela Ban
 * @version $Id: ExponentialIntervalTest.java,v 1.2 2007/08/21 07:18:12 belaban Exp $
 */
public class ExponentialIntervalTest extends TestCase {
    ExponentialInterval interval;

    public ExponentialIntervalTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInitialization() {
        interval=new ExponentialInterval(10);
        System.out.println("interval=" + interval);
        long value=interval.next();
        System.out.println("interval=" + interval);
        assertEquals(10, value);
        value=interval.next();
        System.out.println("interval=" + interval);
        assertEquals(20, value);
    }

    public void testNoargConstructor() {
        interval=new ExponentialInterval();
        assertEquals(30, interval.next());
        assertEquals(60, interval.next());
    }


    public void testMax() {
        interval=new ExponentialInterval(1000);
        System.out.println("interval=" + interval);
        assertEquals(1000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(2000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(4000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(8000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(15000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(15000, interval.next());
        System.out.println("interval=" + interval);

    }

    public static Test suite() {
        return new TestSuite(ExponentialIntervalTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }



}