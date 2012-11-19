package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.FUNCTIONAL)
public class SeqnoListTest {

    public void testAddition() {
        SeqnoList list=new SeqnoList().add(1).add(5,10).add(15);
        System.out.println("list = " + list);
        assert list.size() == 8;
        assert list.getLast() == 15;
    }

    public void testRemoval() {
        SeqnoList list=new SeqnoList().add(1).add(5,10).add(15);
        list.remove(0);
        assert list.size() == 8;

        list.remove(1);
        assert list.size() == 7;

        int size=7;
        for(long seqno: new long[]{5,6,7,8,9,10}) {
            list.remove(seqno);
            assert list.size() == --size;
        }

        assert list.size() == 1;
    }


    public void testRemoval2() {
        SeqnoList list=new SeqnoList(1).add(10,15);
        assert list.size() == 7;

        list.remove(12);
        assert list.size() == 3;
    }


    public void testRemoveHigherThan() {
        SeqnoList list=new SeqnoList(1).add(10,15);
        list.removeHigherThan(15);
        assert list.size() == 7;

        list.removeHigherThan(14);
        assert list.size() == 6;

        list.removeHigherThan(3);
        assert list.size() ==1;
    }

    public void testRemoveHigherThan2() {
        SeqnoList list=new SeqnoList(1).add(10,15);
        list.removeHigherThan(10);
        assert list.size() == 2;
    }



    public void testLast() {
        SeqnoList list=new SeqnoList().add(3).add(5,10);
        assert list.getLast() == 10;
    }

    public void testIteration() {
        SeqnoList list=new SeqnoList().add(1).add(5,10).add(15);
        List<Long> expected=Arrays.asList(1L,5L,6L,7L,8L,9L,10L,15L);
        _testIteration(list, expected);
    }


    public static void testIteration2() {
        _testIteration(new SeqnoList(5, 10), Arrays.asList(5L,6L,7L,8L,9L,10L));
    }


    public static void testIteration3() {
        _testIteration(new SeqnoList(5, 10).add(11), Arrays.asList(5L,6L,7L,8L,9L,10L,11L));
    }

    public static void testIteration4() {
        _testIteration(new SeqnoList(4).add(5, 10),
                       Arrays.asList(4L, 5L,6L,7L,8L,9L,10L));
    }

    public static void testIteration5() {
        _testIteration(new SeqnoList(3).add(5, 10).add(12),
                       Arrays.asList(3L, 5L,6L,7L,8L,9L,10L,12L));
    }

    public static void testIteration6() {
        _testIteration(new SeqnoList(3).add(5, 5).add(7),
                       Arrays.asList(3L,5L,7L));
    }


    public void testSerialization() throws Exception {
        SeqnoList list=new SeqnoList().add(1, 10, 50)
          .add(100,150).add(152,153).add(200,205).add(300,304,306).add(400,450).add(500);
        for(int i=502; i < 550; i+=2)
            list.add(i);
        System.out.println("list.size()=" + list.size()  + "\nlist = " + list);
        int expected_size=list.serializedSize();
        byte[] buf=Util.streamableToByteBuffer(list);
        SeqnoList list2=(SeqnoList)Util.streamableFromByteBuffer(SeqnoList.class,buf);
        System.out.println("list2.size()=" + list2.size() + "\nlist2 = " + list2);
        assert list.size() == list2.size();

        System.out.println("expected size=" + expected_size + ", buf size=" + buf.length);
        assert buf.length == expected_size;
    }

    protected static void _testIteration(SeqnoList list, List<Long> expected) {
        System.out.println("list = " + list);
        assert list.size() == expected.size();
        List<Long> actual=new ArrayList<Long>(expected.size());
        for(long i: list)
            actual.add(i);
        System.out.println("expected=" + expected + "\nactual:  " + actual);
        assert expected.equals(actual);
    }
}
