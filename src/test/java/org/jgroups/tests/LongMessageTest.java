package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.LongMessage;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class LongMessageTest extends MessageTestBase {

    public void testConstructor() {
        LongMessage msg=new LongMessage().setValue(50);
        System.out.println("msg = " + msg);
        assert msg.getValue() == 50;
    }

    public void testGetUndefinedValue() {
        LongMessage msg=new LongMessage();
        long val=msg.getValue();
        assert val == 0;
    }

    public void testGetObject() {
        LongMessage msg=new LongMessage().setValue(22);
        long val=msg.getObject();
        assert val == 22;
    }

    public void testSetObject() {
        LongMessage msg=new LongMessage();
        msg.setObject(22);
        assert msg.getValue() == 22;
        msg.setObject(23L);
        assert msg.getValue() == 23;
        assert msg.getObject() instanceof Long;
    }

    public void testSize() throws Exception {
        LongMessage msg=new LongMessage().setValue(22);
        _testSize(msg);
    }
}
