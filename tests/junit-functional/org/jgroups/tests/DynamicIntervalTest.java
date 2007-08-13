package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.stack.DynamicInterval;


/**
 * @author Bela Ban
 * @version $Id: DynamicIntervalTest.java,v 1.1 2007/08/13 12:30:03 belaban Exp $
 */
public class DynamicIntervalTest extends TestCase {
    DynamicInterval interval;

    public DynamicIntervalTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInitialization() {
        interval=new DynamicInterval(10);
        System.out.println("interval=" + interval);
        long value=interval.next();
        System.out.println("interval=" + interval);
        assertEquals(10, value);
        value=interval.next();
        System.out.println("interval=" + interval);
        assertEquals(20, value);
    }

    public void testNoargConstructor() {
        interval=new DynamicInterval();
        assertEquals(30, interval.next());
        assertEquals(60, interval.next());
    }


    public void testMax() {
        interval=new DynamicInterval(1000);
        System.out.println("interval=" + interval);
        assertEquals(1000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(2000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(4000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(5000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(5000, interval.next());
        System.out.println("interval=" + interval);
        assertEquals(5000, interval.next());
        System.out.println("interval=" + interval);

    }

    public static Test suite() {
        return new TestSuite(DynamicIntervalTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(DynamicIntervalTest.suite());
    }



}