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
    }

    public void testIteration() {
        SeqnoList list=new SeqnoList().add(1).add(5,10).add(15);
        System.out.println("list = " + list);
        int count=0;
        List<Long> generated=new ArrayList<Long>(10);
        for(long num: list) {
            System.out.print(num + " ");
            count++;
            generated.add(num);
        }

        assert count == list.size();

        List<Long> expected=Arrays.asList(1L,5L,6L,7L,8L,9L,10L,15L);

        System.out.println("\nexpected list:  " + expected + "\ngenerated list: " + generated);

        assert expected.equals(generated);
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
}
