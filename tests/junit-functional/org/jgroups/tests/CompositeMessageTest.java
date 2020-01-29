package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * Tests {@link org.jgroups.CompositeMessage}
 * @author Bela Ban
 * @since  5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class CompositeMessageTest extends MessageTestBase {
    protected static final Address SRC=Util.createRandomAddress("X"), DEST=Util.createRandomAddress("A");
    protected static final Message M1=create(DEST, 10, false, false);
    protected static final Message M2=create(DEST, 1000, true, true);
    protected static final Message M3=new EmptyMessage(DEST);

    public void testCreation() {
        CompositeMessage msg=new CompositeMessage(DEST, M1, M2);
        assert msg.getNumberOfMessages() == 2;
        assert msg.getLength() == M1.getLength() + M2.getLength();
    }

    public void testAdd() {
        CompositeMessage msg=new CompositeMessage(null)
          .add(new EmptyMessage(null), new BytesMessage(null, "hello".getBytes()))
          .add(new ObjectMessage(null, "hello world"));
        assert msg.getNumberOfMessages() == 3;
    }

    public void testIteration() {
        CompositeMessage msg=new CompositeMessage(DEST)
          .add(M1, M2, M3, new LongMessage(DEST, 322649));
        int cnt=0;
        Message[] tmp=new Message[msg.getNumberOfMessages()];
        for(Message m: msg)
            tmp[cnt++]=m;
        assert cnt == msg.getNumberOfMessages();
        for(int i=0; i < cnt; i++)
            assert msg.get(i) == tmp[i];
    }

    public void testCopy() {
        CompositeMessage msg=new CompositeMessage(DEST, M1, M2, M3);
        CompositeMessage copy=msg.copy(false, true);
        assert copy.getNumberOfMessages() == 0;
        copy=msg.copy(true, true);
        assert copy.getNumberOfMessages() == 3;
        assert msg.getLength() == copy.getLength();
        assert msg.size() == copy.size();
    }


    protected static Message create(Address dest, int length, boolean nio, boolean direct) {
        if(!nio)
            return new BytesMessage(dest, new byte[length]).setSrc(SRC);
        return direct? new NioMessage(dest, ByteBuffer.allocateDirect(length)).setSrc(SRC) :
          new NioMessage(dest, ByteBuffer.allocate(length)).setSrc(SRC);
    }

}
