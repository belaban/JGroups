package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.nio.Buffers;
import org.jgroups.nio.MockSocketChannel;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Bela Ban
 * @since  3.6.5
 */
@Test(groups=Global.FUNCTIONAL)
public class BuffersTest {
    protected static ByteBuffer buffer() {return ByteBuffer.wrap("hello world".getBytes());}

    public void testCreation() {
        ByteBuffer b=buffer();
        Buffers bufs=new Buffers(b);
        System.out.println("bufs = " + bufs);
        assert bufs.remaining() == Global.INT_SIZE + buffer().capacity();
    }

    public void testWrite() throws Exception {
        ByteBuffer b=buffer();
        Buffers bufs=new Buffers();
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(15);
        boolean rc=bufs.write(ch, b);
        assert rc;

        // write only a portion of the data
        ch.bytesToWrite(10);
        b.clear();
        rc=bufs.write(ch, b);
        assert rc == false;

        ch.bytesToWrite(10);
        rc=bufs.write(ch);
        assert rc;

        ch.bytesToWrite(10);
        b=buffer(); // mimic a new buffer
        rc=bufs.write(ch, b);
        assert rc == false;

        ch.bytesToWrite(30);
        b=buffer(); // mimic a new buffer
        rc=bufs.write(ch, b);
        assert rc;
    }


    public void testRead() throws Exception {
        byte[] data="hello world".getBytes();
        MockSocketChannel ch=new MockSocketChannel()
          .bytesToRead((ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());

        Buffers bufs=new Buffers();
        ByteBuffer b=bufs.read(ch);
        System.out.println("b = " + b);
        assert b != null;
        assert Arrays.equals(data, b.array());
    }

    public void testPartialRead() throws Exception {
        byte[] tmp="hello world".getBytes();
        ByteBuffer data=ByteBuffer.allocate(Global.INT_SIZE + tmp.length).putInt(tmp.length).put(tmp);
        data.flip().limit(2); // read only the first 2 bytes of the length

        MockSocketChannel ch=new MockSocketChannel().bytesToRead(data);
        Buffers bufs=new Buffers();
        ByteBuffer rc=bufs.read(ch);
        assert rc == null;

        data.limit(8); // we can now read the remaining 2 bytes to complete the length, plus 4 bytes into the data
        rc=bufs.read(ch);
        assert rc == null;

        data.limit(14); // this will still not allow the read to complete
        rc=bufs.read(ch);
        assert rc == null;

        data.limit(15); // this will still not allow the read to complete
        rc=bufs.read(ch);
        assert rc != null;
        System.out.println("rc = " + rc);
        assert Arrays.equals(tmp, rc.array());
    }

    public void testEof() throws Exception {
        byte[] data={'B', 'e', 'l', 'a'}; // -1 == EOF
        MockSocketChannel ch=new MockSocketChannel()
          .bytesToRead((ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());
        Buffers bufs=new Buffers();
        ByteBuffer buf=bufs.read(ch);
        assert buf != null;
        assert buf.limit() == data.length;
        ch.doClose();

        try {
            buf=bufs.read(ch);
            assert false : "read() should have thrown an EOFException";
        }
        catch(EOFException eof) {
            System.out.printf("received exception as expected: %s\n", eof);
        }
    }


    public void testReadLength() throws Exception {
        byte[] tmp="hello world".getBytes();
        ByteBuffer data=ByteBuffer.allocate(Global.INT_SIZE + tmp.length).putInt(tmp.length).put(tmp);
        data.flip().limit(4); // read the entire length
        MockSocketChannel ch=new MockSocketChannel().bytesToRead(data);
        Buffers bufs=new Buffers();
        ByteBuffer buf=bufs.read(ch);
        assert buf == null;

        data.limit(8); // allow for some more data to be read...
        buf=bufs.read(ch);
        assert buf == null;

        data.limit(data.capacity()); // read all data
        buf=bufs.read(ch);
        assert buf != null;
    }


}
