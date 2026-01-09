package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Tests {@link ReceiverAdapter}
 * @author Bela Ban
 * @since  3.6.4
 */
@Test(groups=Global.FUNCTIONAL)
public class ReceiverAdapterTest {

    public void testSimpleReceive() {
        testSimpleReceive(ByteBuffer.allocate(50));
        testSimpleReceive(ByteBuffer.allocateDirect(50));
    }

    public void testSimpleWrap() {
        MyReceiver rec=new MyReceiver();
        ByteBuffer buf=ByteBuffer.wrap("Bela Ban".getBytes(), 5, 3);
        rec.receive(null, buf);
        assert rec.offset == 5 && rec.length == 3;
        assert Arrays.equals(rec.buffer, "Ban".getBytes());
    }

    protected void testSimpleReceive(final ByteBuffer buf) {
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

        @Override
        public void receive(Address sender, DataInput in, int length) throws Exception {
            this.buffer=new byte[length];
            in.readFully(buffer, offset=0, this.length=length);
        }

        @Override
        public void receive(Address sender, ByteBuffer buf) {
            this.buffer=new byte[this.length=buf.remaining()];
            this.offset=buf.position();
            buf.get(buffer);
        }

        public String toString() {
            return String.format("buf.length=%d, offset=%d, length=%d", buffer != null? buffer.length : 0, offset, length);
        }
    }
}
