package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.ByteBufferOutputStream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests {@link org.jgroups.util.ByteBufferInputStream}
 * @author Bela Ban
 * @since  5.5.3
 */
@Test(groups=Global.FUNCTIONAL,dataProvider="createByteBuffer")
public class ByteBufferInputStreamTest {

    @DataProvider
    static Object[][] createByteBuffer() {
        return new Object[][]{
          {ByteBuffer.allocate(16)},
          {ByteBuffer.allocateDirect(16)}
        };
    }

    public void testSimpleRead(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        out.writeInt(322649);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        int i=in.readInt();
        assert i == 322649;
    }

    public void testReadArray(ByteBuffer bb) throws IOException {
        byte[] buf="hello world".getBytes();
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        out.write(buf, 0, buf.length);
        byte[] tmp=new byte[16];
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        int len=in.read(tmp, 0, tmp.length);
        String s=new String(tmp, 0, len);
        assert "hello world".equals(s);
    }

    public void testAvailable(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        out.writeInt(322649);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        int available=in.available();
        assert available == 4;
    }

    public void testReadUnsignedByte(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        out.writeByte(-22);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        int b=in.readByte();
        assert b == -22;
        out.buf().flip();
        b=in.readUnsignedByte();
        assert b == (-22 & 0xff);
    }

    public void testReadUnsignedShort(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        out.writeShort(-2222);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        int b=in.readShort();
        assert b == -2222;
        in.buf().flip();
        b=in.readUnsignedShort();
        assert b == (-2222 & 0xffff);
    }

    public void testReadLine(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        byte[] t="hello from\nbela".getBytes();
        out.write(t);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        String line=in.readLine();
        assert "hello from".equals(line);
    }

    public void testReadUTF(ByteBuffer bb) throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(bb);
        final String s="hello world";
        out.writeUTF(s);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        String ss=in.readUTF();
        assert s.equals(ss);
    }
}
