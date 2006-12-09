package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.util.UnmodifiableVector;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;

public class UnmodifiableVectorTest extends TestCase {
    Vector v;
    UnmodifiableVector uv;

    public UnmodifiableVectorTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        v=new Vector();
        v.add("one"); v.add("two");
        uv=new UnmodifiableVector(v);
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testCreation() {
        assertEquals(v.size(), uv.size());
        assertTrue(uv.contains("two"));
    }

    public void testAddition() {
        try {
            uv.add("three");
            fail("should throw an exception");
        }
        catch(UnsupportedOperationException ex) {
            // expected
        }
    }

    public void testRemoval() {
        try {
            uv.add("two");
            fail("should throw an exception");
        }
        catch(UnsupportedOperationException ex) {
            // expected
        }
    }

    public void testIteration() {
        Object el;
        for(Iterator it=uv.iterator(); it.hasNext();) {
            el=it.next();
            System.out.println(el);
        }

        for(Iterator it=uv.iterator(); it.hasNext();) {
            el=it.next();
            try {
                it.remove();
                fail("should throw exception");
            }
            catch(UnsupportedOperationException ex) {
                // expected
            }
        }
    }

    public void testListIteration() {
        Object el;
        for(ListIterator it=uv.listIterator(); it.hasNext();) {
            el=it.next();
            System.out.println(el);
        }

        for(ListIterator it=uv.listIterator(); it.hasNext();) {
            el=it.next();
             try {
                 it.remove();
                 fail("should throw exception");
             }
             catch(UnsupportedOperationException ex) {
                 // expected
             }
        }
    }

    public static void main(String[] args) {
        String[] testCaseName={UnmodifiableVectorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
