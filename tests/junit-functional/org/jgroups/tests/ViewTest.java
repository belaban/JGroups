// $Id: ViewTest.java,v 1.4 2009/05/05 15:13:14 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;


@Test(groups=Global.FUNCTIONAL)
public class ViewTest {
    Address a, b, c, d, e, f, g, h, i, j, k;
    View view;
    List<Address> members;
    

   

    @BeforeClass
    public void setUp() throws Exception {
        a=Util.createRandomAddress();
        b=a;
        c=b;
        d=Util.createRandomAddress();
        e=Util.createRandomAddress();
        f=Util.createRandomAddress();
        g=Util.createRandomAddress();
        h=Util.createRandomAddress();
        i=Util.createRandomAddress();
        ViewId id=new ViewId(a, 34);
        members=Arrays.asList(a, b, d, e, f, g, h);
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
        assert a.equals(view.getCreator()) : "Creator should be a";
        assert !view.getCreator().equals(d) : "Creator should not be d";
    }

    public void testEquals() {
        assert view.equals(view);
    }

    public void testEquals2() {
        View v1=new View(new ViewId(a, 12345), new Vector<Address>(members));
        View v2=new View(a, 12345, new Vector<Address>(members));
        assert v1.equals(v2);
        View v3=new View(a, 12543, new Vector<Address>(members));
        assert !v1.equals(v3);
    }


    public static void testEquals3() {
        View v1=new View(), v2=new View();
        assert v1.equals(v2);
    }



}
