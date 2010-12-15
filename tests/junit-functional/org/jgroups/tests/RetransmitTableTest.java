package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.RetransmitTable;
import org.testng.annotations.Test;

/** Tests {@link org.jgroups.util.RetransmitTable}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class RetransmitTableTest {
    static final Message MSG=new Message(null, null, "test");

    public static void testCreation() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        int size=table.size();
        assert size == 0;
        assert table.get(15) == null;
    }


    public static void testAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");
        addAndGet(table, 11, "11");
        addAndGet(table, 19, "19");
        addAndGet(table, 20, "20");
        addAndGet(table, 29, "29");
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assert table.size() == table.computeSize();
        assert table.capacity() == 30;
    }


    public static void testAdditionWithOffset() {
        RetransmitTable table=new RetransmitTable(3, 10, 100);
        addAndGet(table, 100, "100");
        addAndGet(table, 101, "101");
        addAndGet(table, 105, "105");
        addAndGet(table, 109, "109");
        addAndGet(table, 110, "110");
        addAndGet(table, 111, "111");
        addAndGet(table, 119, "119");
        addAndGet(table, 120, "120");
        addAndGet(table, 129, "129");
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assert table.capacity() == 30;
    }
    

    public static void testDuplicateAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");

        assert !table.put(5, new Message());
        assert table.get(5).getObject().equals("5");
        assert table.size() == 5;
    }


    public static void testDumpMatrix() {
        RetransmitTable table=new RetransmitTable(3, 10, 1);
        long[] seqnos={1,3,5,7,9,12,14,16,18,20,21,22,23,24};
        for(long seqno: seqnos)
            table.put(seqno, MSG);
        System.out.println("matrix:\n" + table.dumpMatrix());
    }


    public static void testMassAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        final int NUM_MSGS=10005;
        final Message MSG=new Message(null, null, "hello world");
        for(int i=0; i < NUM_MSGS; i++)
            table.put(i, MSG);
        System.out.println("table = " + table);
        assert table.size() == NUM_MSGS;
        assert table.capacity() == 10010;
    }

    public static void testResize() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        assert table.capacity() == 30;
        addAndGet(table, 30, "30");
        addAndGet(table, 35, "35");
        assert table.capacity() == 40;
        addAndGet(table, 500, "500");
        assert table.capacity() == 510;

        addAndGet(table, 515, "515");
        assert table.capacity() == 520;
    }


    public static void testPurge() {
        RetransmitTable table=new RetransmitTable(5, 10, 0);
        for(long seqno=0; seqno < 25; seqno++)
            table.put(seqno, MSG);

        long[] seqnos={30,31,32,37,38,39, 40,41,42,47,48,49};
        for(long seqno: seqnos)
            table.put(seqno, MSG);

        System.out.println("table (before remove):\n" + table.dumpMatrix());
        for(long seqno=0; seqno <= 22; seqno++)
            table.remove(seqno);

        System.out.println("table (after remove, before purge):\n" + table.dumpMatrix());
        table.purge(22);
        System.out.println("table: (after purge):\n" + table.dumpMatrix());

        assert table.size() == 2 + seqnos.length;
    }


    protected static void addAndGet(RetransmitTable table, long seqno, String message) {
        boolean added=table.put(seqno, new Message(null, null, message));
        assert added;
        Message msg=table.get(seqno);
        assert msg != null && msg.getObject().equals(message);
    }

}
