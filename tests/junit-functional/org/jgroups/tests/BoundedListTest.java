
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.util.BoundedList;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups=Global.FUNCTIONAL)
public class BoundedListTest {


    public static void testAdd() throws Exception {
        BoundedList<Integer> list=new BoundedList<>(3);
        Assert.assertEquals(0, list.size());
        list.add(1);
        System.out.println(list);
        Assert.assertEquals(1, list.size());

        list.add(2);
        System.out.println(list);

        list.add(3);
        System.out.println(list);
        Assert.assertEquals(3, list.size());

        list.add(4);
        System.out.println(list);
        Assert.assertEquals(3, list.size());


        int tmp;

        tmp=list.removeFromHead();
        Assert.assertEquals(2, tmp);

        tmp=list.removeFromHead();
        Assert.assertEquals(3, tmp);

        tmp=list.removeFromHead();
        Assert.assertEquals(4, tmp);
    }



    public static void testContains() throws Exception {
        BoundedList<String> strlist=new BoundedList<>(3);
        strlist.add("Bela");
        System.out.println(strlist);

        strlist.add("Michelle");
        System.out.println(strlist);

        strlist.add("Jeannette");
        System.out.println(strlist);

        strlist.add("Nicole");
        System.out.println(strlist);

        assert !(strlist.contains("Bela"));
        assert strlist.contains("Nicole");
        assert strlist.contains("Michelle");
    }

    
    public static void testWithManyElements() {
        BoundedList<Integer> list=new BoundedList<>(3);
        for(int i=0; i < 100000; i++) {
            list.add(i);
        }
        System.out.println("list: " + list);
        Assert.assertEquals(3, list.size());
    }



}
