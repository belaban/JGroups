package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups=Global.FUNCTIONAL)
public class SeqnoListTest {

    public void testAddition() {
        SeqnoList list=new SeqnoList(16, 5000);
        list.add(5001).add(5005,5010).add(5015);
        System.out.println("list = " + list);
        assert list.size() == 8;
        assert list.getLast() == 5015;
    }

    public void testAddWithOffset() {
        SeqnoList list=new SeqnoList(100, 300000);
        list.add(300000,300050);
        assert list.size() == 51;
        assert list.getLast() == 300050;
        list.removeHigherThan(300020);
        assert list.size() == 21;
    }

    public void testAdd2() {
        long first_seqno=500, last_seqno=500;
        SeqnoList list=new SeqnoList((int)(last_seqno - first_seqno +1), first_seqno);
        list.add(first_seqno, last_seqno);
        System.out.println("list = " + list);
        assert list.size() == 1;
    }

    public void testGetLast() {
        SeqnoList list=new SeqnoList(20);
        assert list.getLast() == -1;
        list.add(0);
        assert list.getLast() == 0;
        list.add(5,7);
        assert list.getLast() == 7;
        list.add(15,19);
        assert list.getLast() == 19;

        list=new SeqnoList(15).add(3).add(5,10);
        assert list.getLast() == 10;
    }


    public void testRemoveHigherThan() {
        SeqnoList list=new SeqnoList(20).add(1).add(10, 15);
        list.removeHigherThan(15);
        assert list.size() == 7;

        list.removeHigherThan(14);
        assert list.size() == 6;

        list.removeHigherThan(3);
        assert list.size() ==1;
    }

    public void testRemoveHigherThan2() {
        SeqnoList list=new SeqnoList(20).add(1).add(10, 15);
        list.removeHigherThan(10);
        assert list.size() == 2;
    }


    public void testIteration() {
        SeqnoList list=new SeqnoList(16).add(1).add(5,10).add(15);
        List<Long> expected=Arrays.asList(1L,5L,6L,7L,8L,9L,10L,15L);
        _testIteration(list, expected);
    }


    public static void testIteration2() {
        _testIteration(new SeqnoList(15).add(5,10), Arrays.asList(5L,6L,7L,8L,9L,10L));
    }


    public static void testIteration3() {
        _testIteration(new SeqnoList(12).add(5,10).add(11), Arrays.asList(5L,6L,7L,8L,9L,10L,11L));
    }

    public static void testIteration4() {
        _testIteration(new SeqnoList(11).add(4).add(5, 10), Arrays.asList(4L, 5L,6L,7L,8L,9L,10L));
    }

    public static void testIteration5() {
        _testIteration(new SeqnoList(15).add(3).add(5, 10).add(12), Arrays.asList(3L, 5L,6L,7L,8L,9L,10L,12L));
    }

    public static void testIteration6() {
        _testIteration(new SeqnoList(8).add(3).add(5, 5).add(7), Arrays.asList(3L,5L,7L));
    }


    public void testSerialization() throws Exception {
        SeqnoList list=new SeqnoList(1000).add(1, 10, 50)
          .add(100,150).add(152,153).add(200,205).add(300,304,306).add(400,450).add(500);
        for(int i=502; i < 550; i+=2)
            list.add(i);
        System.out.println("list.size()=" + list.size()  + "\nlist = " + list);
        int expected_size=list.serializedSize();
        byte[] buf=Util.streamableToByteBuffer(list);
        SeqnoList list2=Util.streamableFromByteBuffer(SeqnoList::new, buf);
        System.out.println("list2.size()=" + list2.size() + "\nlist2 = " + list2);
        assert list.size() == list2.size();

        System.out.println("expected size=" + expected_size + ", buf size=" + buf.length);
        assert buf.length == expected_size;

        Iterator<Long> it1=list.iterator(), it2=list2.iterator();
        while(it1.hasNext()) {
            long seq1=it1.next(), seq2=it2.next();
            assert seq1 == seq2;
        }
    }

    public void testSerialization2() {
        SeqnoList list=new SeqnoList(8000);
        for(int i=0; i < 8000; i++)
            list.add(i);
        System.out.println("list = " + list);
        assert list.size() == 8000;
        int serialized_size=list.serializedSize();
        System.out.println("serialized_size = " + serialized_size);
    }

    public void testSerialization3() {
        int max_bundle_size=64000;
        int estimated_max_msgs_in_xmit_req=(max_bundle_size -50) * Global.LONG_SIZE;

        SeqnoList list=new SeqnoList(estimated_max_msgs_in_xmit_req);
        for(int i=0; i < estimated_max_msgs_in_xmit_req; i++)
            list.add(i);
        System.out.println("list = " + list);
        assert list.size() == estimated_max_msgs_in_xmit_req;
        int serialized_size=list.serializedSize();
        System.out.println("serialized_size = " + serialized_size);
        assert serialized_size <= max_bundle_size;
    }



    protected static void _testIteration(SeqnoList list, List<Long> expected) {
        System.out.println("list = " + list);
        assert list.size() == expected.size();
        List<Long> actual=new ArrayList<>(expected.size());
        for(long i: list)
            actual.add(i);
        System.out.println("expected=" + expected + "\nactual:  " + actual);
        assert expected.equals(actual);
    }
}
