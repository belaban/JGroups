package org.jgroups.tests;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.jgroups.Global;
import org.jgroups.nio.MessageReader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * 
 * @author Christian Fredriksson
 * @since 5.5.3
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageReaderTest {
    protected ByteBuffer buf1;
    protected ByteBuffer buf2;
    protected ByteBuffer buf3;

    @BeforeMethod protected void setup() {
        buf1 = ByteBuffer.wrap("hello world".getBytes());
        buf2 = ByteBuffer.wrap(" from Bela".getBytes());
        buf3 = ByteBuffer.wrap("foo".getBytes());
    }

    public void testRead() throws Exception {
        byte[] data = "hello world".getBytes();
        MockSocketChannel ch = new MockSocketChannel()
                .bytesToRead(ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());

        MessageReader bufs = new MessageReader(ch);
        ByteBuffer b = bufs.readMessage();
        System.out.println("b = " + b);
        assert b != null;
        assert Arrays.equals(data, array(b));
    }

    public void testPartialRead() throws Exception {
        byte[] tmp = "hello world".getBytes();
        ByteBuffer data = ByteBuffer.allocate(Global.INT_SIZE + tmp.length).putInt(tmp.length).put(tmp);
        data.flip().limit(2); // read only the first 2 bytes of the length

        MockSocketChannel ch = new MockSocketChannel().bytesToRead(data);
        MessageReader bufs = new MessageReader(ch);
        ByteBuffer rc = bufs.readMessage();
        assert rc == null;

        data.limit(8); // we can now read the remaining 2 bytes to complete the length, plus 4 bytes into the data
        rc = bufs.readMessage();
        assert rc == null;

        data.limit(14); // this will still not allow the read to complete
        rc = bufs.readMessage();
        assert rc == null;

        data.limit(15); // this will still not allow the read to complete
        rc = bufs.readMessage();
        assert rc != null;
        System.out.println("rc = " + rc);
        assert Arrays.equals(tmp, array(rc));
    }

    public void testEof() throws Exception {
        byte[] data = {'B', 'e', 'l', 'a'}; // -1 == EOF
        MockSocketChannel ch = new MockSocketChannel()
                .bytesToRead(ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());
        MessageReader bufs = new MessageReader(ch);
        ByteBuffer buf = bufs.readMessage();
        assert buf != null;
        assert buf.limit() == data.length;
        ch.doClose();

        try {
            buf = bufs.readMessage();
            assert false : "read() should have thrown an EOFException";
        } catch (EOFException eof) {
            System.out.printf("received exception as expected: %s\n", eof);
        }
    }

    public void testReadLength() throws Exception {
        byte[] tmp = "hello world".getBytes();
        ByteBuffer data = ByteBuffer.allocate(Global.INT_SIZE + tmp.length).putInt(tmp.length).put(tmp);
        data.flip().limit(4); // read the entire length
        MockSocketChannel ch = new MockSocketChannel().bytesToRead(data);
        MessageReader bufs = new MessageReader(ch);
        ByteBuffer buf = bufs.readMessage();
        assert buf == null;

        data.limit(8); // allow for some more data to be read...
        buf = bufs.readMessage();
        assert buf == null;

        data.limit(data.capacity()); // read all data
        buf = bufs.readMessage();
        assert buf != null;
    }

    private static byte[] array(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }
}
