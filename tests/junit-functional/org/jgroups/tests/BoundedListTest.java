
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.util.BoundedList;


public class BoundedListTest extends TestCase {
    private BoundedList<Integer> list=null;
    private BoundedList<String> strlist=null;

    public BoundedListTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        list=new BoundedList<Integer>(3);
        strlist=new BoundedList<String>(3);
    }


    public void tearDown() throws Exception {
        super.tearDown();
        list.clear();
        list=null;
    }


    public void testAdd() throws Exception {
        assertEquals(0, list.size());
        list.add(new Integer(1));
        System.out.println(list);
        assertEquals(1, list.size());

        list.add(new Integer(2));
        System.out.println(list);

        list.add(new Integer(3));
        System.out.println(list);
        assertEquals(3, list.size());

        list.add(new Integer(4));
        System.out.println(list);
        assertEquals(3, list.size());


        int tmp;

        tmp=list.removeFromHead().intValue();
        assertEquals(2, tmp);

        tmp=list.removeFromHead().intValue();
        assertEquals(3, tmp);

        tmp=list.removeFromHead().intValue();
        assertEquals(4, tmp);
    }



    public void testContains() throws Exception {
        strlist.add("Bela");
        System.out.println(strlist);

        strlist.add("Michelle");
        System.out.println(strlist);

        strlist.add("Jeannette");
        System.out.println(strlist);

        strlist.add("Nicole");
        System.out.println(strlist);

        assertFalse(strlist.contains("Bela"));
        assertTrue(strlist.contains("Nicole"));
        assertTrue(strlist.contains("Michelle"));
    }

    public void testWithManyElements() {
        for(int i=0; i < 100000; i++) {
            list.add(i);
        }
        System.out.println("list: " + list);
        assertEquals(3, list.size());
    }

    public static void main(String[] args) {
        String[] testCaseName={BoundedListTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
