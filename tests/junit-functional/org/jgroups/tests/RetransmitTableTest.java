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
        RetransmitTable table=new RetransmitTable(3, 10);
        assert table.size() == 0;
        assert table.get(15) == null;
    }


    public static void testAddition() {
        RetransmitTable table=new RetransmitTable(3, 10);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");
        addAndGet(table, 11, "11");
        addAndGet(table, 19, "19");
        addAndGet(table, 20, "20");
        addAndGet(table, 29, "29");
        System.out.println("table: " + table);
        assert table.size() == 9;
        assert table.capacity() == 30;
    }

    public static void testDuplicateAddition() {
        RetransmitTable table=new RetransmitTable(3, 10);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");

        assert !table.add(5, new Message());
        assert table.get(5).getObject().equals("5");
        assert table.size() == 5;
    }

    public static void testResize() {
        RetransmitTable table=new RetransmitTable(3, 10);
        assert table.capacity() == 30;
        addAndGet(table, 30, "30");
        addAndGet(table, 35, "35");
        assert table.capacity() == 40;
    }


    protected static void addAndGet(RetransmitTable table, long seqno, String message) {
        boolean added=table.add(seqno, new Message(null, null, message));
        assert added;
        Message msg=table.get(seqno);
        assert msg != null && msg.getObject().equals(message);
    }

}
