package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.nio.MockSocketChannel;
import org.jgroups.nio.WriteBuffers;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Tests {@link org.jgroups.nio.WriteBuffers}
 * @author Bela Ban
 * @since  3.6.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class WriteBuffersTest {
    protected ByteBuffer buf1;
    protected ByteBuffer buf2;
    protected ByteBuffer buf3;

    protected static final Method makeSpace, spaceAvailable, nullData, add;
    protected static final Field  position, limit;

    static {
        try {
            makeSpace=WriteBuffers.class.getDeclaredMethod("makeSpace");
            makeSpace.setAccessible(true);
            spaceAvailable=WriteBuffers.class.getDeclaredMethod("spaceAvailable");
            spaceAvailable.setAccessible(true);
            nullData=WriteBuffers.class.getDeclaredMethod("nullData");
            nullData.setAccessible(true);
            add=WriteBuffers.class.getDeclaredMethod("add", ByteBuffer.class);
            add.setAccessible(true);
            position=Util.getField(WriteBuffers.class, "position");
            position.setAccessible(true);
            limit=Util.getField(WriteBuffers.class, "limit");
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


    public void testCreation() {
        WriteBuffers buf=new WriteBuffers(4);
        check(buf, 0, 0, 0, 0);

        buf=new WriteBuffers(buf1);
        check(buf, 0, 2, 2, buf1.limit() + Global.INT_SIZE);
    }


    public void testAdd() throws Exception {
        WriteBuffers buf=new WriteBuffers(3);
        add(buf, buf1);
        check(buf, 0, 2, 2, remaining(buf1));

        add(buf, buf2);
        check(buf, 0, 4, 4, remaining(buf1, buf2));

        add(buf, buf3);
        check(buf, 0, 6, 6, remaining(buf1, buf2, buf3));
    }


    public void testMakeSpaceSimple() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        WriteBuffers buf=new WriteBuffers(3);
        add(buf, buf1);
        check(buf, 0, 2, 2, remaining(buf1));
        boolean rc=makeSpace(buf);
        assert !rc;

        buf.write(ch);
        check(buf, 2, 2, 0, 0);

        rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 0, 0, 0);
    }

    public void testMakeSpaceSimpleMove() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        WriteBuffers buf=new WriteBuffers(3);
        add(buf, buf1);
        buf.write(ch);
        check(buf, 2, 2, 0, 0);

        add(buf, buf2);
        check(buf, 2, 4, 2, remaining(buf2));

        boolean rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 2, 2, remaining(buf2));
    }

    public void testMakeSpaceOverlappingMove() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        WriteBuffers buf=new WriteBuffers(3);
        add(buf, buf1);
        check(buf, 0, 2, 2, remaining(buf1));
        buf.write(ch);
        check(buf, 2, 2, 0, 0);

        add(buf, buf2);
        add(buf, buf3);

        boolean rc=makeSpace(buf);
        assert rc;
        check(buf, 0, 4, 4, remaining(buf2,buf3));
    }

    public void testNullData() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        WriteBuffers buf=new WriteBuffers(3);
        for(ByteBuffer b: Arrays.asList(buf1,buf2,buf3))
            add(buf, b);
        check(buf, 0, 6, 6, remaining(buf1, buf2, buf3));
        boolean rc=buf.write(ch);
        assert rc;
        check(buf, 0, 0, 0, 0);
    }


    public void testSimpleWrite() throws Exception {
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(1000);
        WriteBuffers buf=new WriteBuffers(2);
        boolean rc=buf.write(ch, buf1.duplicate());
        assert rc;
        check(buf, 2, 2, 0, 0);

        rc=buf.write(ch, buf2.duplicate());
        assert rc;
        check(buf, 0, 0, 0, 0);
    }

    public void testWrite() throws Exception {
        ByteBuffer b=buf1.duplicate();
        WriteBuffers bufs=new WriteBuffers();
        MockSocketChannel ch=new MockSocketChannel().bytesToWrite(15);
        boolean rc=bufs.write(ch, b);
        assert rc;
        check(bufs, 0, 0, 0, 0);

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
        check(bufs, 1, 2, 1, b.remaining());

        ch.bytesToWrite(30);
        b=buf1.duplicate(); // mimic a new buffer
        rc=bufs.write(ch, b);
        assert rc;
        check(bufs, 1, 1, 0, 0);
    }

    public void testWriteWithBuffering() throws Exception {
        WriteBuffers bufs=new WriteBuffers(4);
        MockSocketChannel ch=new MockSocketChannel(); // all writes will fail
        for(int i=0; i < 4; i++) {
            ByteBuffer b=buf1.duplicate();
            boolean rc=bufs.write(ch, b);
            assert !rc;
        }
        check(bufs, 0, 8, 8, remaining(buf1) *4);

        ch.bytesToWrite(20);
        boolean rc=bufs.write(ch);
        assert !rc;

        rc=bufs.write(ch, buf1.duplicate());
        assert !rc;
        check(bufs, 0, 7, 7, remaining(buf1) * 5 - 20);

        ch.bytesToWrite(1000); // write all buffers now
        rc=bufs.write(ch);
        assert rc;
        check(bufs, 7 ,7 ,0 ,0);

        rc=bufs.write(ch, buf1.duplicate());
        assert rc;
        check(bufs, 2, 2, 0, 0);
    }

    public void testWriteAtStartup() throws Exception {
        WriteBuffers bufs=new WriteBuffers(5);
        MockSocketChannel ch=new MockSocketChannel(); // all writes will initially fail
        boolean rc=bufs.write(ch, buf1);
        assert !rc;
        check(bufs, 0, 2, 2, remaining(buf1));

        ch.bytesToWrite(100);
        rc=bufs.write(ch, buf2);
        assert rc;
        check(bufs, 4, 4, 0, 0);

        for(int i=0; i < 3; i++) {
            ByteBuffer b=buf3.duplicate();
            rc=bufs.write(ch, b);
            assert rc;
        }
        check(bufs, 0, 0, 0, 0);
    }


    protected void check(WriteBuffers buf, int pos, int limit, int size, int remaining) {
        System.out.println("buf = " + buf);
        assert buf.position() == pos : String.format("expected %d but got %d", pos, buf.position());
        assert buf.limit() == limit : String.format("expected %d but got %d", limit, buf.limit());
        assert buf.size() == size : String.format("expected %d but got %d", size, buf.size());
        assert buf.remaining() == remaining : String.format("expected %d but got %d", remaining, buf.remaining());
    }

    protected boolean makeSpace(WriteBuffers buf) throws Exception {
        return (boolean)makeSpace.invoke(buf);
    }

    protected boolean spaceAvailable(WriteBuffers buf) throws Exception {
        return (boolean)spaceAvailable.invoke(buf);
    }

    protected boolean nullData(WriteBuffers buf) throws Exception {
        return (boolean)nullData.invoke(buf);
    }

    protected void add(WriteBuffers buf, ByteBuffer data) throws Exception {
        add.invoke(buf, data);
    }

    protected int remaining(ByteBuffer ... buffers) {
        int remaining=0;
        for(ByteBuffer buf: buffers) {
            remaining+=buf.remaining() + Global.INT_SIZE;
        }
        return remaining;
    }

    protected void position(WriteBuffers buf, int pos) {
        Util.setField(position, buf, pos);
    }

    protected void limit(WriteBuffers buf, int lim) {
        Util.setField(limit, buf, lim);
    }

}
