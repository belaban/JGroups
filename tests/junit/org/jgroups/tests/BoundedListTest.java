// $Id: BoundedListTest.java,v 1.1 2003/11/21 06:53:05 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.util.BoundedList;


public class BoundedListTest extends TestCase {
    private BoundedList l=null;

    public BoundedListTest(String name) {
        super(name);
    }

    public void setUp() {
        l=new BoundedList(3);
    }


    public void tearDown() {
        l.removeAll();
        l=null;
    }


    public void testAdd() throws Exception {
        assertEquals(l.size(), 0);
        l.add(new Integer(1));
        System.out.println(l);
        assertEquals(l.size(), 1);

        l.add(new Integer(2));
        System.out.println(l);

        l.add(new Integer(3));
        System.out.println(l);
        assertEquals(l.size(), 3);

        l.add(new Integer(4));
        System.out.println(l);
        assertEquals(l.size(), 3);


        int tmp;

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 2);

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 3);

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 4);
    }


    public void testAddAtHead() throws Exception {
        assertEquals(l.size(), 0);
        l.addAtHead(new Integer(1));
        System.out.println(l);
        assertEquals(l.size(), 1);

        l.addAtHead(new Integer(2));
        System.out.println(l);

        l.addAtHead(new Integer(3));
        System.out.println(l);
        assertEquals(l.size(), 3);

        l.addAtHead(new Integer(4));
        System.out.println(l);
        assertEquals(l.size(), 3);

        int tmp;

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 4);

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 3);

        tmp=((Integer)l.removeFromHead()).intValue();
        assertEquals(tmp, 2);
    }


    public void testContains() throws Exception {
        l.add("Bela");
        System.out.println(l);

        l.add("Michelle");
        System.out.println(l);

        l.add("Jeannette");
        System.out.println(l);

        l.add("Nicole");
        System.out.println(l);

        assertTrue(l.contains("Bela") == false);
        assertTrue(l.contains("Nicole"));
        assertTrue(l.contains("Michelle"));
    }

    public static void main(String[] args) {
        String[] testCaseName={BoundedListTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
