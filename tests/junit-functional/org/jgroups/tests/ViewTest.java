
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Test(groups=Global.FUNCTIONAL)
public class ViewTest {
    Address a, b, c, d, e, f, g, h, i;
    View view;
    List<Address> members;
    

   

    @BeforeClass
    void setUp() throws Exception {
        a=Util.createRandomAddress("A");
        b=a;
        c=b;
        d=Util.createRandomAddress("D");
        e=Util.createRandomAddress("E");
        f=Util.createRandomAddress("F");
        g=Util.createRandomAddress("G");
        h=Util.createRandomAddress("H");
        i=Util.createRandomAddress("I");
        ViewId id=new ViewId(a, 34);
        members=Arrays.asList(a, b, d, e, f, g, h);
        view=new View(id, members);

    }

    public void testGetMembers() throws Exception {
        List<Address> mbrs=view.getMembers();
        try {
            mbrs.add(a);
            assert false: "adding a member to a view should throw an exception";
        }
        catch(UnsupportedOperationException ex) {
            System.out.println("adding a member threw " + ex.getClass().getSimpleName() + " as expected");
        }

        byte[] buf=Util.objectToByteBuffer(view);
        View view2=(View)Util.objectFromByteBuffer(buf);
        System.out.println("view2 = " + view2);

        mbrs=view2.getMembers();
        try {
            mbrs.add(a);
            assert false: "adding a member to a view should throw an exception";
        }
        catch(UnsupportedOperationException ex) {
            System.out.println("adding a member threw " + ex.getClass().getSimpleName() + " as expected");
        }
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
        View v1=new View(new ViewId(a, 12345), new ArrayList<Address>(members));
        View v2=new View(a, 12345, new ArrayList<Address>(members));
        assert v1.equals(v2);
        View v3=new View(a, 12543, new ArrayList<Address>(members));
        assert !v1.equals(v3);
    }
 

    public void testCopy() throws Exception {
        View view2=view.copy();
        System.out.println("view = " + view);
        System.out.println("view2 = " + view2);
        assert view.equals(view2);

        List<Address> mbrs=view2.getMembers();
        try {
            mbrs.add(a);
            assert false: "adding a member to a view should throw an exception";
        }
        catch(UnsupportedOperationException ex) {
            System.out.println("adding a member threw " + ex.getClass().getSimpleName() + " as expected");
        }
    }



}
