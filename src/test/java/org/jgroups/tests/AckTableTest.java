package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.AckTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests {@link org.jgroups.util.AckTable}
 * @author Bela Ban
 * @since  5.4
 */
@Test(groups=Global.FUNCTIONAL)
public class AckTableTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");

    public void testAdjust() {
        AckTable table=new AckTable();
        assert table.size() == 0;
        assert table.min() == 0;

        table.adjust(List.of(a, b, c, d));
        assert table.size() == 4;
    }

    public void testAck() {
        AckTable table=new AckTable();
        long rc=table.ack(a, 200)[1];
        assert rc == 200;
        assert table.size() == 1;
        assert table.min() == 200;

        rc=table.ack(a, 200)[1];
        assert rc == 200;
        assert table.size() == 1;
        assert table.min() == 200;

        table.ack(a, 150); // wil be ignored
        assert table.min() == 200;

        rc=table.ack(b, 250)[1];
        assert rc == 200;
        assert table.size() == 2;
        rc=table.ack(b, 150)[1];
        assert table.size() == 2;
        assert table.min() == 200;

        table.ack(c, 500);
        rc=table.ack(d, 600)[1];
        assert rc == 200;

        table.adjust(List.of(a,c,d));
        assert table.size() == 3;
        assert table.min() == 200;

        table.adjust(List.of(c,d));
        assert table.size() == 2;
        assert table.min() == 500;

        table.adjust(List.of(a,b,c,d));
        assert table.size() == 4;
        assert table.min() == 0;

        table.clear();
        assert table.size() == 0;
        assert table.min() == 0;
    }

    public void testAck2() {
        AckTable table=new AckTable();
        long[] rc=table.ack(a, 200);
        assert rc[0] == 0;
        assert rc[1] == 200;
        assert table.size() == 1;
        assert table.min() == 200;

        rc=table.ack(a, 200);
        assert rc[0] == 200 && rc[1] == 200;
        assert table.size() == 1;
        assert table.min() == 200;

        rc=table.ack(a, 150); // wil be ignored
        assert rc[0] == 200 && rc[1] == 200;
        assert table.min() == 200;

        rc=table.ack(b, 250);
        assert rc[0] == 200 && rc[1] == 200;
        assert table.size() == 2;
        rc=table.ack(b, 150);
        assert rc[0] == 200 && rc[1] == 200;
        assert table.size() == 2;
        assert table.min() == 200;

        table.ack(c, 500);
        rc=table.ack(d, 600);
        assert rc[0] == 200 && rc[1] == 200;

        table.adjust(List.of(a,c,d));
        assert table.size() == 3;
        assert table.min() == 200;

        table.adjust(List.of(c,d));
        assert table.size() == 2;
        assert table.min() == 500;

        table.adjust(List.of(a,b,c,d));
        assert table.size() == 4;
        assert table.min() == 0;

        table.clear();
        assert table.size() == 0;
        assert table.min() == 0;
    }
}
