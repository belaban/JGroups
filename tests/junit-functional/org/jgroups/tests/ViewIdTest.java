// $Id: ViewIdTest.java,v 1.1 2007/07/04 07:29:33 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.ViewId;

import java.net.InetAddress;


public class ViewIdTest extends TestCase {
    ViewId v1, v2, v3, v4;


    public ViewIdTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        try {
            v1=new ViewId(new org.jgroups.stack.IpAddress(InetAddress.getByName("localhost"), 1000), 22);
            v2=new ViewId(new org.jgroups.stack.IpAddress(InetAddress.getByName("localhost"), 1000), 21);
            v3=(ViewId)v1.clone();
        }
        catch(Exception e) {
            System.err.println("ViewIdTest.setUp(): " + e);
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        v1=v2=v3=null;
    }


    public void test0() {
        assertTrue(v1.equals(v2) == false);
    }

    public void test1() {
        assertEquals(v1, v3);
    }


    public void test2() {
        v3=(ViewId)v1.clone();
        assertEquals(v1, v3);
    }


    public void test3() {
        assertTrue(v1.compareTo(v3) == 0);
    }


    public void test4() {
        assertTrue(v1.compareTo(v2) > 0);
    }


    public void test5() {
        assertTrue(v2.compareTo(v1) < 0);
    }


    public static Test suite() {
        TestSuite s=new TestSuite(ViewIdTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


