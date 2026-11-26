
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.util.BoundedList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

@Test(groups=Global.FUNCTIONAL)
public class BoundedListTest {


    public void testAdd() throws Exception {
        BoundedList<Integer> list=new BoundedList<>(3);
        Assert.assertEquals(list.size(), 0);
        for(int i=1; i <= 3; i++) {
            list.add(i);
            System.out.println(list);
            Assert.assertEquals(list.size(), i);
        }

        list.add(4);
        System.out.println(list);
        Assert.assertEquals(list.size(), 3);
        List<Integer> expected=IntStream.rangeClosed(2, 4).boxed().toList(), actual=list.contents();
        assert actual.equals(expected);

        int tmp=list.removeFromHead();
        Assert.assertEquals(tmp, 2);

        tmp=list.removeFromHead();
        Assert.assertEquals(tmp, 3);

        tmp=list.removeFromHead();
        Assert.assertEquals(tmp, 4);
        assert list.isEmpty();
    }


    public void testContains() throws Exception {
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

    
    public void testWithManyElements() {
        BoundedList<Integer> list=new BoundedList<>(3);
        for(int i=0; i < 100_000; i++) {
            list.add(i);
        }
        System.out.println("list: " + list);
        Assert.assertEquals(list.size(), 3);
    }



}
