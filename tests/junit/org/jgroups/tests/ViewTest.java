// $Id: ViewTest.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;


import junit.framework.*;
import org.jgroups.*;
import org.jgroups.stack.IpAddress;



public class ViewTest extends TestCase
{
    IpAddress a, b, c, d, e, f, g, h, i, j, k;
    View view;

    public ViewTest(String Name_)
    {
        super(Name_);
    } //public ViewTest(String Name_)

    public void setUp()
    {
        a=new IpAddress("localhost", 5555);
        b=new IpAddress("localhost", 5555);
        c=b;
        d=new IpAddress("localhost", 5556);
        e=new IpAddress("127.0.0.1", 5555);
        f=new IpAddress("www.ibm.com", 80);
        g=new IpAddress("www.ibm.com", 8080);
        h=new IpAddress("224.0.0.1", 5555);
        i=new IpAddress("224.0.0.2", 5555);
        ViewId id = new ViewId( a , 34 );
        java.util.Vector members = new java.util.Vector();
        members.addElement(a);
        members.addElement(b);
        members.addElement(d);
        members.addElement(e);
        members.addElement(f);
        members.addElement(g);
        members.addElement(h);
        view = new View(id, members);

    }

    public void testContainsMember()
    {
        assertTrue("Member should be in view",view.containsMember(a));
        assertTrue("Member should be in view",view.containsMember(b));
        assertTrue("Member should be in view",view.containsMember(c));
        assertTrue("Member should be in view",view.containsMember(d));
        assertTrue("Member should be in view",view.containsMember(e));
        assertTrue("Member should be in view",view.containsMember(f));
        assertTrue("Member should not be in view",!view.containsMember(i));
    }

    public void testEqualsCreator()
    {
        assertTrue( "Creator should be a:", view.getCreator().equals(a));
        assertTrue( "Creator should not be d", !view.getCreator().equals(d));
    }

    public void tearDown()
    {
        a = null;
        b = null;
        c = null;
        d = null;
        e = null;
        f = null;
        g = null;
        h = null;
        i = null;
        view = null;
    }

    public static void main(String[] args)
    {
        String[] testCaseName = {ViewTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    } //public static void main(String[] args)

} //public class ViewTest extends TestCase
