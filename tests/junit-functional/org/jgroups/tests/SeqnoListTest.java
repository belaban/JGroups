package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

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
        for(long num: list) {
            System.out.print(num + " ");
            count++;
        }

        assert count == list.size();
    }

    public void testSerialization() throws Exception {
        SeqnoList list=new SeqnoList().add(1, 10, 50)
          .add(100,150).add(152,153).add(200,205).add(300,304,306).add(400,450).add(500);
        for(int i=502; i < 550; i+=2)
            list.add(i);


        System.out.println("list.size()=" + list.size()  + "\nlist = " + list);
        byte[] buf=Util.streamableToByteBuffer(list);
        SeqnoList list2=(SeqnoList)Util.streamableFromByteBuffer(SeqnoList.class,buf);
        System.out.println("list2.size()=" + list2.size() + "\nlist2 = " + list2);
        assert list.size() == list2.size();
    }
}
