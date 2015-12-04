package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.nio.Buffers;
import org.jgroups.nio.MockSocketChannel;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Bela Ban
 * @since  3.6.5
 */
@Test(groups=Global.FUNCTIONAL)
public class BuffersTest {
    protected static final Method makeSpace, spaceAvailable, nullData;
    protected static final Field position, limit;
    protected ByteBuffer         buf1;
    protected ByteBuffer         buf2;
    protected ByteBuffer         buf3;

    static {
        try {
            makeSpace=Buffers.class.getDeclaredMethod("makeSpace");
            makeSpace.setAccessible(true);
            spaceAvailable=Buffers.class.getDeclaredMethod("spaceAvailable", int.class);
            spaceAvailable.setAccessible(true);
            nullData=Buffers.class.getDeclaredMethod("nullData");
            nullData.setAccessible(true);
            position=Util.getField(Buffers.class, "position");
            position.setAccessible(true);
            limit=Util.getField(Buffers.class, "limit");
            limit.setAccessible(true);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeMethod protected void setup() {
        buf1=ByteBuffer.wrap("hello world".getBytes());
        buf2=ByteBuffer.wrap(" from Bela".getBytes());
        buf3=ByteBuffer.wrap("foo".getBytes());
    }


    protected static ByteBuffer buffer() {return ByteBuffer.wrap("hello world".getBytes());}

    public void testCreation() {
        ByteBuffer b=buffer();
        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), b);
        System.out.println("bufs = " + bufs);
        assert bufs.remaining() == Global.INT_SIZE + b.capacity();

        Buffers buf=new Buffers(8);
        check(buf, 0, 0, 0, 0);

        buf=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), buf1);
        check(buf, 0, 2, 2, buf1.limit() + Global.INT_SIZE);
    }


    public void testRead() throws Exception {
        byte[] data="hello world".getBytes();
        MockSocketChannel ch=new MockSocketChannel()
          .bytesToRead((ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());

        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
        ByteBuffer b=bufs.readLengthAndData(ch);
        System.out.println("b = " + b);
        assert b != null;
        assert Arrays.equals(data, b.array());
    }

    public void testPartialRead() throws Exception {
        byte[] tmp="hello world".getBytes();
        ByteBuffer data=ByteBuffer.allocate(Global.INT_SIZE + tmp.length).putInt(tmp.length).put(tmp);
        data.flip().limit(2); // read only the first 2 bytes of the length

        MockSocketChannel ch=new MockSocketChannel().bytesToRead(data);
        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
        ByteBuffer rc=bufs.readLengthAndData(ch);
        assert rc == null;

        data.limit(8); // we can now read the remaining 2 bytes to complete the length, plus 4 bytes into the data
        rc=bufs.readLengthAndData(ch);
        assert rc == null;

        data.limit(14); // this will still not allow the read to complete
        rc=bufs.readLengthAndData(ch);
        assert rc == null;

        data.limit(15); // this will still not allow the read to complete
        rc=bufs.readLengthAndData(ch);
        assert rc != null;
        System.out.println("rc = " + rc);
        assert Arrays.equals(tmp, rc.array());
    }

    public void testEof() throws Exception {
        byte[] data={'B', 'e', 'l', 'a'}; // -1 == EOF
        MockSocketChannel ch=new MockSocketChannel()
          .bytesToRead((ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE + data.length).putInt(data.length).put(data).flip());
        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
        ByteBuffer buf=bufs.readLengthAndData(ch);
        assert buf != null;
        assert buf.limit() == data.length;
        ch.doClose();

        try {
            buf=bufs.readLengthAndData(ch);
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
        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
        ByteBuffer buf=bufs.readLengthAndData(ch);
        assert buf == null;

        data.limit(8); // allow for some more data to be read...
        buf=bufs.readLengthAndData(ch);
        assert buf == null;

        data.limit(data.capacity()); // read all data
        buf=bufs.readLengthAndData(ch);
        assert buf != null;
    }

    /** Reads | version (short) | number (int) | cookie [4 bytes] | */
    public void testRead3Buffers() throws Exception {
        byte[] cookie={'b', 'e', 'l', 'a'};
        int num=322649;
        ByteBuffer input=(ByteBuffer)ByteBuffer.allocate(Global.SHORT_SIZE + cookie.length + Global.INT_SIZE)
          .putShort(Version.version)
          .putInt(num).put(cookie, 0, cookie.length).flip();
        MockSocketChannel ch=new MockSocketChannel().bytesToRead(input);
        Buffers bufs=new Buffers(ByteBuffer.allocate(2), ByteBuffer.allocate(4), ByteBuffer.allocate(4));
        boolean rc=bufs.read(ch);
        System.out.println("bufs = " + bufs);
        assert rc;
        assert bufs.position() == 3;
        assert bufs.limit() == 3;

        for(ByteBuffer b: bufs) // reset position so data can be read
            b.clear();

        short version=bufs.get(0).getShort(0);
        assert version == Version.version;

        int num2=bufs.get(1).getInt(0);
        assert num2 == num;

        byte[] tmp=new byte[4];
        bufs.get(2).get(tmp, 0, tmp.length);
        assert Arrays.equals(tmp, cookie);
    }


    public void testAdd() throws Exception {
        Buffers buf=new Buffers(12);
        buf.add(buf1);
        check(buf, 0, 1, 1, remaining(buf1));

        buf.add(buf2);
        check(buf, 0, 2, 2, remaining(buf1, buf2));

        buf.add(buf3);
        check(buf, 0, 3, 3, remaining(buf1, buf2, buf3));
    }


    public void testMakeSpaceSimple() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        Buffers buf=new Buffers(2);
        buf.add(buf1);
        check(buf, 0, 1, 1, remaining(buf1));
        boolean rc=makeSpace(buf);
        assert !rc;

        buf.write(ch);
        check(buf, 1, 1, 0, 0);

        rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 0, 0, 0);
    }

    public void testMakeSpaceSimpleMove() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        Buffers buf=new Buffers(6);
        buf.add(buf1);
        buf.write(ch);
        check(buf, 1, 1, 0, 0);

        buf.add(buf2);
        check(buf, 1, 2, 1, remaining(buf2));

        boolean rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 1, 1, remaining(buf2));
    }

    public void testMakeSpaceOverlappingMove() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        Buffers buf=new Buffers(6);
        buf.add(buf1);
        check(buf, 0, 1, 1, remaining(buf1));
        buf.write(ch);
        check(buf, 1, 1, 0, 0);

        buf.add(buf2, buf3);

        boolean rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 2, 2, remaining(buf2,buf3));
    }

    public void testNullData() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        Buffers buf=new Buffers(3);
        for(ByteBuffer b: Arrays.asList(buf1,buf2,buf3))
            buf.add(b);
        check(buf, 0, 3, 3, remaining(buf1, buf2, buf3));
        boolean rc=buf.write(ch);
        assert rc;
        // makeSpace(buf);
        check(buf, 0, 0, 0, 0);
    }


    public void testSimpleWrite() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        Buffers buf=new Buffers(2);
        boolean rc=buf.write(ch, buf1.duplicate());
        assert rc;
        check(buf, 1, 1, 0, 0);

        rc=buf.write(ch, buf2.duplicate());
        assert rc;
        check(buf, 0, 0, 0, 0);
    }

    public void testWrite() throws Exception {
        ByteBuffer b=buf1.duplicate();
        Buffers bufs=new Buffers(2);
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(15);
        boolean rc=bufs.write(ch, b);
        assert rc;
        check(bufs, 1, 1, 0, 0);

        // write only a portion of the data
        ch.bytesToWrite(10);
        b.clear();
        rc=bufs.write(ch, b);
        assert !rc;
        check(bufs, 1, 2, 1, b.remaining());

        ch.bytesToWrite(10);
        rc=bufs.write(ch);
        assert rc;
        check(bufs, 0, 0, 0, 0);

        ch.bytesToWrite(10);
        b=buf1.duplicate(); // mimic a new buffer
        rc=bufs.write(ch, b);
        assert rc == false;
        check(bufs, 0, 1, 1, b.remaining());

        ch.bytesToWrite(30);
        b=buf1.duplicate(); // mimic a new buffer
        rc=bufs.write(ch, b);
        assert rc;
        check(bufs, 0, 0, 0, 0);
    }

    public void testWrite2() throws Exception {
        ByteBuffer b=buffer();
        Buffers bufs=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
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

    public void testWriteWithBuffering() throws Exception {
        Buffers bufs=new Buffers(4);
        MockSocketChannel ch=new MockSocketChannel(); // all writes will fail
        for(int i=0; i < 4; i++) {
            ByteBuffer b=buf1.duplicate();
            boolean rc=bufs.write(ch, b);
            assert !rc;
        }
        check(bufs, 0, 4, 4, remaining(buf1) *4);

        ch.bytesToWrite(20);
        boolean rc=bufs.write(ch);
        assert !rc;

        rc=bufs.write(ch, buf1.duplicate());
        assert !rc;
        check(bufs, 0, 4, 4, remaining(buf1) * 5 - 20);

        ch.bytesToWrite(1000); // write all buffers now
        rc=bufs.write(ch);
        assert rc;
        check(bufs, 0 ,0 ,0 ,0);

        rc=bufs.write(ch, buf1.duplicate());
        assert rc;
        check(bufs, 1, 1, 0, 0);
    }

    public void testWriteAtStartup() throws Exception {
        Buffers bufs=new Buffers(5);
        MockSocketChannel ch=new MockSocketChannel(); // all writes will initially fail
        boolean rc=bufs.write(ch, buf1);
        assert !rc;
        check(bufs, 0, 1, 1, remaining(buf1));

        ch.bytesToWrite(100);
        rc=bufs.write(ch, buf2);
        assert rc;
        check(bufs, 2, 2, 0, 0);

        for(int i=0; i < 3; i++) {
            ByteBuffer b=buf3.duplicate();
            rc=bufs.write(ch, b);
            assert rc;
        }
        check(bufs, 0, 0, 0, 0);
    }

    public void testIteration() {
        Buffers buf=new Buffers(6).add(ByteBuffer.wrap("hello world".getBytes()), ByteBuffer.allocate(1024),
                                       ByteBuffer.allocate(500), ByteBuffer.allocate(1024));
        int i=1, count=0;
        for(ByteBuffer b: buf) {
            System.out.printf("buffer #%d: %d bytes\n", i++, b.remaining());
            count++;
        }
        assert count == 4;
        System.out.println("");

        buf.remove(0).remove(2);
        i=1; count=0;
        for(ByteBuffer b: buf) {
            System.out.printf("buffer #%d: %d bytes\n", i++, b.remaining());
            count++;
        }
        assert count == 2;

        buf=new Buffers(10);
        for(i=1; i <= 10; i++)
            buf.add(ByteBuffer.allocate(i));
        buf.remove(3).remove(4).remove(5).remove(9);

        System.out.println();
        count=0; i=1;
        for(ByteBuffer b: buf) {
            System.out.printf("buffer #%d: %d bytes\n", i++, b.remaining());
            count++;
        }
        assert count == 6;
    }


    protected void check(Buffers buf, int pos, int limit, int size, int remaining) {
        System.out.println("buf = " + buf);
        assert buf.position() == pos : String.format("expected %d but got %d", pos, buf.position());
        assert buf.limit() == limit : String.format("expected %d but got %d", limit, buf.limit());
        assert buf.size() == size : String.format("expected %d but got %d", size, buf.size());
        assert buf.remaining() == remaining : String.format("expected %d but got %d", remaining, buf.remaining());
    }

    protected boolean makeSpace(Buffers buf) throws Exception {
        return (boolean)makeSpace.invoke(buf);
    }

    protected boolean spaceAvailable(Buffers buf, int num_bufs) throws Exception {
        return (boolean)spaceAvailable.invoke(buf, num_bufs);
    }

    protected boolean nullData(Buffers buf) throws Exception {
        return (boolean)nullData.invoke(buf);
    }

    protected int remaining(ByteBuffer ... buffers) {
        int remaining=0;
        for(ByteBuffer buf: buffers) {
            remaining+=buf.remaining();
        }
        return remaining;
    }

    protected void position(Buffers buf, int pos) {
        Util.setField(position, buf, pos);
    }

    protected void limit(Buffers buf, int lim) {
        Util.setField(limit, buf, lim);
    }

}
