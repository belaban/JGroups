package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.UnmodifiableVector;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;


@Test(groups=Global.FUNCTIONAL)
public class UnmodifiableVectorTest {


    public static void testCreation() {
        Vector v=new Vector(Arrays.asList("one", "two"));
        UnmodifiableVector uv=new UnmodifiableVector(v);
        Assert.assertEquals(v.size(), uv.size());
        assert uv.contains("two");
    }

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public static void testAddition() {
        Vector v=new Vector(Arrays.asList("one", "two"));
        UnmodifiableVector uv=new UnmodifiableVector(v);
        uv.add("three");
    }

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public static void testRemoval() {
        Vector v=new Vector(Arrays.asList("one", "two"));
        UnmodifiableVector uv=new UnmodifiableVector(v);
        uv.add("two");
    }

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public static void testIteration() {
        Vector v=new Vector(Arrays.asList("one", "two"));
        UnmodifiableVector uv=new UnmodifiableVector(v);
        Object el;
        for(Iterator it=uv.iterator(); it.hasNext();) {
            el=it.next();
            System.out.println(el);
        }

        for(Iterator it=uv.iterator(); it.hasNext();) {
            el=it.next();
            it.remove();
        }
    }

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public static void testListIteration() {
        Vector v=new Vector(Arrays.asList("one", "two"));
        UnmodifiableVector uv=new UnmodifiableVector(v);
        Object el;
        for(ListIterator it=uv.listIterator(); it.hasNext();) {
            el=it.next();
            System.out.println(el);
        }

        for(ListIterator it=uv.listIterator(); it.hasNext();) {
            el=it.next();
            it.remove();
        }
    }


}
