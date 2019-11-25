package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Tests {@link ReceiverAdapter}
 * @author Bela Ban
 * @since  3.6.4
 */
@Test(groups=Global.FUNCTIONAL)
public class ReceiverAdapterTest {

    public static void testSimpleReceive() {
        testSimpleReceive(ByteBuffer.allocate(50));
        testSimpleReceive(ByteBuffer.allocateDirect(50));
    }

    public static void testSimpleWrap() {
        MyReceiver rec=new MyReceiver();
        ByteBuffer buf=ByteBuffer.wrap("Bela Ban".getBytes(), 5, 3);
        rec.receive(null, buf);
        assert rec.offset == 5 && rec.length == 3;
        byte[] tmp=new byte[3];
        buf.get(tmp);
        assert Arrays.equals(tmp, "Ban".getBytes());
    }

    protected static void testSimpleReceive(final ByteBuffer buf) {
        MyReceiver rec=new MyReceiver();
        buf.putInt(1).putInt(2).putInt(3);
        buf.flip();
        rec.receive(null, buf);
        assert rec.offset == 0 && rec.length == Global.INT_SIZE *3;
    }

    protected static final class MyReceiver extends ReceiverAdapter {
        protected byte[] buffer;
        protected int    offset;
        protected int    length;


        public void receive(Address sender, byte[] buf, int offset, int length) {
            this.buffer=buf;
            this.offset=offset;
            this.length=length;
        }

        public String toString() {
            return String.format("buf.length=%d, offset=%d, length=%d", buffer != null? buffer.length : 0, offset, length);
        }
    }
}
