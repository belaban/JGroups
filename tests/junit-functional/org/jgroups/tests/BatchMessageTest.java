package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * Tests {@link BatchMessage}
 * @author Bela Ban
 * @since  5.3.7, 5.4
 */
@Test(groups=Global.FUNCTIONAL)
public class BatchMessageTest extends MessageTestBase {
    protected static final Address SRC=Util.createRandomAddress("X"), DEST=Util.createRandomAddress("A");
    protected static final Message M1=create(DEST, 10, false, false);
    protected static final Message M2=create(DEST, 1000, true, true);
    protected static final Message M3=new EmptyMessage(DEST);

    public void testCreation() {
        BatchMessage msg=new BatchMessage(DEST, SRC, new Message[]{M1,M2,M3}, 3);
        assert msg.getNumberOfMessages() == 3;
        assert msg.getLength() == M1.getLength() + M2.getLength() + M3.getLength();
    }

    /** https://issues.redhat.com/browse/JGRP-2788 */
    public void testSendMulticast() throws Exception {
        try(JChannel a=new JChannel(Util.getTestStack()).name("A");
            JChannel b=new JChannel(Util.getTestStack()).name("B")) {
            MyReceiver<Message> r1=new MyReceiver<Message>().rawMsgs(true);
            MyReceiver<Message> r2=new MyReceiver<Message>().rawMsgs(true);
            a.connect(BatchMessageTest.class.getSimpleName());
            b.connect(BatchMessageTest.class.getSimpleName());
            a.setReceiver(r1);
            b.setReceiver(r2);
            Util.waitUntilAllChannelsHaveSameView(2000, 100, a,b);
            BatchMessage msg=new BatchMessage(null, 3);
            for(int i=1; i <= 5; i++)
                msg.add(new ObjectMessage(null, "hello-" + i));
            System.out.print("-- sending multicast BatchMessage: ");
            a.send(msg);
            System.out.println(": done");
            Util.waitUntil(2000, 100, () -> r1.size() == 1 && r2.size() == 1);
        }

    }


    protected static Message create(Address dest, int length, boolean nio, boolean direct) {
        if(!nio)
            return new BytesMessage(dest, new byte[length]).setSrc(SRC);
        return direct? new NioMessage(dest, ByteBuffer.allocateDirect(length)).setSrc(SRC) :
          new NioMessage(dest, ByteBuffer.allocate(length)).setSrc(SRC);
    }

}
