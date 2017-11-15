package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.ByteBufferOutputStream;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

/**
 * Tests {@link ByteBufferOutputStream}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class ByteBufferOutputStreamTest {

    public void testConstruction() throws Exception {
        Address dest=Util.createRandomAddress("A");
        Message msg=new BytesMessage(dest, "hello world")
          .setFlag(Message.Flag.DONT_BUNDLE, Message.Flag.OOB).putHeader((short)22, NakAckHeader2.createMessageHeader(322649));
        int size=msg.size();
        ByteBuffer buf=ByteBuffer.allocate(size);
        ByteBufferOutputStream out=new ByteBufferOutputStream(buf);
        msg.writeTo(out);

        buf.flip();
        byte[] array=new byte[buf.limit()];
        System.arraycopy(buf.array(), buf.arrayOffset(), array, 0, buf.limit());
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(array));
        Message copy=new BytesMessage(false);
        copy.readFrom(in);
        System.out.println("copy = " + copy);
        assert msg.getDest() != null && msg.getDest().equals(dest);
    }
}
