// $Id: ViewTest.java,v 1.3 2008/03/10 15:39:22 belaban Exp $

package org.jgroups.tests;


import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.util.Vector;


@Test(groups=Global.FUNCTIONAL)
public class ViewTest {
    IpAddress a, b, c, d, e, f, g, h, i, j, k;
    View view;
    Vector members;
    

    public ViewTest(String Name_) {
    }

    @BeforeClass
    public void setUp() throws Exception {
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
        assert view.containsMember(a) : "Member should be in view";
        assert view.containsMember(b) : "Member should be in view";
        assert view.containsMember(c) : "Member should be in view";
        assert view.containsMember(d) : "Member should be in view";
        assert view.containsMember(e) : "Member should be in view";
        assert view.containsMember(f) : "Member should be in view";
        assert !view.containsMember(i) : "Member should not be in view";
    }

    public void testEqualsCreator() {
        Assert.assertEquals(a, view.getCreator(), "Creator should be a:");
        assert !view.getCreator().equals(d) : "Creator should not be d";
    }

    public void testEquals() {
        Assert.assertEquals(view, view);
    }

    public void testEquals2() {
        View v1=new View(new ViewId(a, 12345), (Vector)members.clone());
        View v2=new View(a, 12345, (Vector)members.clone());
        Assert.assertEquals(v1, v2);
        View v3=new View(a, 12543, (Vector)members.clone());
        assert !(v1.equals(v3));
    }


    public static void testEquals3() {
        View v1, v2;
        v1=new View();
        v2=new View();
        Assert.assertEquals(v1, v2);
    }



}
