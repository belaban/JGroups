package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.SizeCountingInputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;

/**
 * Tests {@link org.jgroups.util.SizeCountingInputStream}
 * @author Bela Ban
 * @since  5.5.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SizeCountingInputStreamTest {
    protected ByteArrayDataInputStream input;
    protected final byte[]             buf="hello world".getBytes();

    @BeforeMethod protected void setup() {
        input=new ByteArrayDataInputStream(buf);
    }

    public void testReadSingleByte() throws IOException {
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        in.read();
        assert in.position() == 1;
        in.read();
        assert in.position() == 2;
        while(in.read() != -1)
            ;
        assert in.position() == buf.length;
    }

    public void testReadSingle0Value() throws IOException {
        byte[] array={'b', 0, 1, 2};
        SizeCountingInputStream in=new SizeCountingInputStream(new ByteArrayDataInputStream(array));
        int pos=0;
        for(int expected: array) {
            int b=in.read();
            assert b == expected;
            assert in.position() == ++pos;
        }
    }

    public void testReadByteArray() throws IOException {
        byte[] tmp=new byte[5];
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        int num=in.read(tmp);
        assert num == 5;
        assert in.position() == 5;

        num=in.read(tmp, 0, 3);
        assert num == 3;
        assert in.position() == 8;

        num=in.read(tmp, 0, tmp.length);
        assert num == 3;
        assert in.position() == buf.length;

        int b=in.read();
        assert b == -1;
        assert in.position() == buf.length;
    }

    public void testReadNBytes() throws IOException {
        byte[] tmp=new byte[5];
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        int count=in.readNBytes(tmp, 0, tmp.length);
        assert count == tmp.length;
        assert in.position() == tmp.length;
    }

    public void testReadNBytes2() throws IOException {
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        byte[] tmp=in.readNBytes(16);
        assert tmp.length == buf.length;
        assert in.position() == tmp.length;
    }

    public void testSkip() throws IOException {
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        long count=in.skip(16);
        assert count == buf.length;
        assert in.position() == buf.length;
    }


    public void testSkipNBytes() throws IOException {
        SizeCountingInputStream in=new SizeCountingInputStream(input);
        try {
            in.skipNBytes(16);
        }
        catch(EOFException eof) {

        }
        assert in.position() == buf.length;
    }


}
