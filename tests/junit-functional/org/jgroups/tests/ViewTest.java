// $Id: ViewTest.java,v 1.2 2007/08/08 10:14:00 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.stack.IpAddress;

import java.util.Vector;


public class ViewTest extends TestCase {
    IpAddress a, b, c, d, e, f, g, h, i, j, k;
    View view;
    Vector members;
    

    public ViewTest(String Name_) {
        super(Name_);
    }

    public void setUp() throws Exception {
        super.setUp();
        a=new IpAddress("localhost", 5555);
        b=new IpAddress("localhost", 5555);
        c=b;
        d=new IpAddress("localhost", 5556);
        e=new IpAddress("127.0.0.1", 5555);
        f=new IpAddress("127.0.0.1", 80);
        g=new IpAddress("127.0.0.1", 8080);
        h=new IpAddress("224.0.0.1", 5555);
        i=new IpAddress("224.0.0.2", 5555);
        ViewId id=new ViewId(a, 34);
        members=new java.util.Vector();
        members.addElement(a);
        members.addElement(b);
        members.addElement(d);
        members.addElement(e);
        members.addElement(f);
        members.addElement(g);
        members.addElement(h);
        view=new View(id, members);

    }

    public void testContainsMember() {
        assertTrue("Member should be in view", view.containsMember(a));
        assertTrue("Member should be in view", view.containsMember(b));
        assertTrue("Member should be in view", view.containsMember(c));
        assertTrue("Member should be in view", view.containsMember(d));
        assertTrue("Member should be in view", view.containsMember(e));
        assertTrue("Member should be in view", view.containsMember(f));
        assertTrue("Member should not be in view", !view.containsMember(i));
    }

    public void testEqualsCreator() {
        assertEquals("Creator should be a:", view.getCreator(), a);
        assertTrue("Creator should not be d", !view.getCreator().equals(d));
    }

    public void testEquals() {
        assertEquals(view, view);
    }

    public void testEquals2() {
        View v1=new View(new ViewId(a, 12345), (Vector)members.clone());
        View v2=new View(a, 12345, (Vector)members.clone());
        assertEquals(v1, v2);
        View v3=new View(a, 12543, (Vector)members.clone());
        assertFalse(v1.equals(v3));
    }


    public void testEquals3() {
        View v1, v2;
        v1=new View();
        v2=new View();
        assertEquals(v1, v2);
    }

    public void tearDown() throws Exception {
        a=null;
        b=null;
        c=null;
        d=null;
        e=null;
        f=null;
        g=null;
        h=null;
        i=null;
        view=null;
        super.tearDown();
    }

    public static void main(String[] args) {
        String[] testCaseName={ViewTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    } //public static void main(String[] args)

} //public class ViewTest extends TestCase
