
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Test(groups=Global.FUNCTIONAL)
public class ViewTest {
    protected Address       a, b, c, d, e, f, g, h, i;
    protected View          view;
    protected List<Address> members;
    

   

    @BeforeClass
    void setUp() throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
        d=Util.createRandomAddress("D");
        e=Util.createRandomAddress("E");
        f=Util.createRandomAddress("F");
        g=Util.createRandomAddress("G");
        h=Util.createRandomAddress("H");
        i=Util.createRandomAddress("I");
        members=Arrays.asList(a, b, c, d, e, f, g, h);
        view=View.create(a, 34, a, b, c, d, e, f, g, h);

    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testConstructor() {
        view=new View(a, 1, null);
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
        View view2=Util.objectFromByteBuffer(buf);
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

    public void testContainsMembers() {
        assert view.containsMembers(b,a,d,c);
        assert !view.containsMembers(a,b,d,f, Util.createRandomAddress("X"));

        View v=View.create(a,1,a,b,c);
        assert v.containsMembers(a,b);

        v=View.create(a,2,a,b);
        assert !v.containsMembers(a,b,c);
    }

    public void testEqualsCreator() {
        assert a.equals(view.getCreator()) : "Creator should be a";
        assert !view.getCreator().equals(d) : "Creator should not be d";
    }

    public void testEquals() {
        View tmp=view;
        assert view.equals(tmp); // tests '=='
    }

    public void testEquals2() {
        View v1=new View(new ViewId(a, 12345), new ArrayList<>(members));
        View v2=new View(a, 12345, new ArrayList<>(members));
        assert v1.equals(v2);
        View v3=new View(a, 12543, new ArrayList<>(members));
        assert !v1.equals(v3);
    }

    /** Tests {@link View#sameViews(View...)} */
    public void testSameViews() {
        View v1=View.create(a,1,a,b,c), v2=View.create(a,1,a,b,c), v3=View.create(a,1,a,b,c), v4=View.create(b,1,a,b,c);
        assert View.sameViews(v1,v2,v3);
        assert !View.sameViews(v1,v2,v3,v4);

        assert View.sameViews(Arrays.asList(v1,v2,v3));
        assert !View.sameViews(Arrays.asList(v1,v2,v3,v4));
    }
 

    public void testCopy() throws Exception {
        View view2=view;
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


    public void testDiff() {
        View one=null;
        View two=View.create(a, 1, a, b, c);
        Address[][] diff=View.diff(one,two);
        System.out.println("diffs: " + printDiff(diff));
        Address[] joined=diff[0], left=diff[1];
        assert joined.length == 3;
        assert joined[0].equals(a) && joined[1].equals(b) && joined[2].equals(c);
        assert left.length == 0;
    }

    public void testDiff2() {
        View one=View.create(a, 1, a,b,c);
        View two=View.create(a, 2, a,b,c,d,e);
        Address[][] diff=View.diff(one,two);
        System.out.println("diffs: " + printDiff(diff));
        Address[] joined=diff[0], left=diff[1];
        assert joined.length == 2;
        assert joined[0].equals(d) && joined[1].equals(e);
        assert left.length == 0;
    }

    public void testDiff3() {
        View one=View.create(a, 1, a,b,c,d,e);
        View two=View.create(a, 2, a,b,c);
        Address[][] diff=View.diff(one,two);
        System.out.println("diffs: " + printDiff(diff));
        Address[] joined=diff[0], left=diff[1];
        assert joined.length == 0;
        assert left.length == 2;
        assert left[0].equals(d) && left[1].equals(e);
    }

    public void testDiff4() {
        View one=View.create(a, 1, a,b,c,d,e,f,g);
        View two=View.create(b, 2, b,c,d,g,h,i);
        Address[][] diff=View.diff(one,two);
        System.out.println("diffs: " + printDiff(diff));
        Address[] joined=diff[0], left=diff[1];
        assert joined.length == 2;
        assert joined[0].equals(h) && joined[1].equals(i);

        assert left.length == 3;
        assert left[0].equals(a) && left[1].equals(e) && left[2].equals(f);
    }

    public void testDiffSameView() {
        View one=View.create(a, 1, a,b,c,d,e,f,g);
        View two=View.create(a, 1, a,b,c,d,e,f,g);
        Address[][] diff=View.diff(one, two);
        assert diff[0].length == 0;
        assert diff[1].length == 0;
    }

    public void testSameMembers() {
        View one=View.create(a, 1, a,b,c,d,e);
        View two=View.create(a, 2, a,b,c);
        assert !View.sameMembers(one, two);

        two=View.create(a, 20, a,b,c,d,e);
        assert View.sameMembers(one, two);
        assert View.sameMembers(two, one);

        one=two;
        assert View.sameMembers(one, two);

        one=View.create(b, 5, a,b,c);
        two=View.create(c, 5, b,a,c);
        assert View.sameMembers(one,two);

        two=View.create(c, 5, b,a,c,d);
        assert !View.sameMembers(one,two);

        one=View.create(b, 5, d,b,c,a);
        two=View.create(c, 5, b,a,c);

        assert !View.sameMembers(one, two);
        assert !View.sameMembers(two, one);
    }

    public void testSameMembersOrdered() {
        View one=View.create(a, 1, a, b, c, d, e);
        View two=View.create(a, 2, a, b, c);
        assert !View.sameMembersOrdered(one, two);
        two=one;
        assert View.sameMembersOrdered(one, two);

        one=View.create(a, 1, a, b, c, d, e);
        two=View.create(a, 5, a, b, c, d, e); // view id is not matched
        assert View.sameMembersOrdered(one, two);
    }


    public void testLeftMembers() {
        View one=View.create(a, 1, a,b,c,d),
          two=View.create(b, 2, c,d);

        List<Address> left=View.leftMembers(one, two);
        System.out.println("left = " + left);
        assert left != null;
        assert left.size() == 2;
        assert left.contains(a);
        assert left.contains(b);
    }

    public void testLeftMembers2() {
        View one=View.create(a, 1, a,b,c,d),
          two=View.create(b, 2, c,d,a,b);
        List<Address> left=View.leftMembers(one, two);
        System.out.println("left = " + left);
        assert left != null;
        assert left.isEmpty();
    }

    public void testNewMembers() {
        View one=View.create(a, 1, a,b),
          two=View.create(b, 2, a,b,c,d);

        List<Address> new_mbrs=View.newMembers(one, two);
        System.out.println("new = " + new_mbrs);
        assert new_mbrs != null;
        assert new_mbrs.size() == 2;
        assert new_mbrs.contains(c);
        assert new_mbrs.contains(d);
    }

    public void testIterator() {
        List<Address> mbrs=new ArrayList<>(members.size());
        for(Address addr: view)
            mbrs.add(addr);

        System.out.println("mbrs: " + mbrs);
        Assert.assertEquals(members, mbrs);
    }

    protected static String printDiff(Address[][] diff) {
        StringBuilder sb=new StringBuilder();
        Address[] joined=diff[0], left=diff[1];
        sb.append("joined: ").append(Arrays.toString(joined)).append(", left: ").append(Arrays.toString(left));
        return sb.toString();
    }

}
