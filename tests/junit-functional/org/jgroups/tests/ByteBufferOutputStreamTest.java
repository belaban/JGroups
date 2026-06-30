package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        Message copy=new BytesMessage();
        copy.readFrom(in);
        System.out.println("copy = " + copy);
        assert msg.getDest() != null && msg.getDest().equals(dest);
    }

    public void testBufferExpansion() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(8);
        for(int i=1; i <= 5; i++)
            out.writeInt(i);
        assert out.buf().capacity() >= 5 * Integer.BYTES;
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(int i=1; i <= 5; i++) {
            int num=in.readInt();
            assert num == i;
        }
    }

    public void testConstructors() throws Exception {
        ByteBufferOutputStream out=new ByteBufferOutputStream(32);
        byte[] tmp="hello world".getBytes();
        out.writeInt(tmp.length);
        out.write(tmp);
        ByteBuffer input_buf=out.buf().flip();

        ByteBufferInputStream in=new ByteBufferInputStream(input_buf);
        int length=in.readInt();
        byte[] tmp2=new byte[length];
        in.read(tmp2, 0, tmp2.length);
        System.out.println("length=" + length + ", " + new String(tmp2));
        assert length == tmp.length;
        assert Arrays.equals(tmp, tmp2);
    }

    public void testLimit() {
        byte[] buf=new byte[100];
        buf[0]='B'; buf[1]='e'; buf[2]='l'; buf[3]='a';
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf));
        assert in.position() == 0;
        assert in.limit() == buf.length;
        assert in.capacity() == buf.length;

        in=new ByteBufferInputStream(ByteBuffer.wrap(buf, 0, 4));
        assert in.position() == 0;
        assert in.limit() == 4;
        assert in.capacity() == buf.length;
        byte[] tmp=new byte[4];
        in.read(tmp, 0, tmp.length);
        assert "Bela".equals(new String(tmp));
    }

    public void testOffsetAndLimit() {
        byte[] buf=new byte[100];
        buf[10]='B'; buf[11]='e'; buf[12]='l'; buf[13]='a';
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf, 10, 4));
        assert in.position() == 10;
        assert in.limit() == 14;
        assert in.capacity() == buf.length;
        byte[] tmp=new byte[4];
        in.read(tmp, 0, tmp.length);
        assert "Bela".equals(new String(tmp));
    }

    public void testOffsetAndLimit2() throws IOException {
        final byte[] data={'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        ByteBufferOutputStream out=new ByteBufferOutputStream(4);
        out.write(data, 6, 4);
        byte[] tmp=Arrays.copyOfRange(data, 6, 10);
        byte[] buf=new byte[4];
        ByteBuffer o=out.buf().duplicate().flip();
        o.get(buf);
        assert Arrays.equals(tmp, buf);
    }

    public void testReadBeyondLimit() {
        byte[] buf=new byte[100];
        buf[10]='B'; buf[11]='e'; buf[12]='l'; buf[13]='a';
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf, 10, 4));
        assert in.position() == 10;
        assert in.limit() == 14;
        assert in.capacity() == buf.length;
        byte[] tmp=new byte[5];
        try {
            in.readFully(tmp, 0, tmp.length);
            assert false : " should have gotten an exception";
        }
        catch(Exception ex) {
            System.out.println("caught exception as expected: " + ex);
            assert ex instanceof EOFException;
        }

        in.buf().position(10);
        for(int i=0; i < 4; i++) in.read();
        assert in.read() == -1;
    }

    public void testReadBeyondLimit2() {
        byte[] buf=new byte[100];
        buf[10]='B'; buf[11]='e'; buf[12]='l'; buf[13]='a';
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, 10, 4);
        assert in.position() == 10;
        assert in.limit() == 14;
        assert in.capacity() == buf.length;
        byte[] tmp=new byte[5];
        try {
            in.readFully(tmp, 0, tmp.length);
            assert false : " should have gotten an exception";
        }
        catch(Exception ex) {
            System.out.println("caught exception as expected: " + ex);
            assert ex instanceof EOFException;
        }
    }

    public void testExpanding() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1);
        for(int i='A'; i < 'A' + 20; i++)
            out.write(i);
        assert out.position() == 20;

        byte[] buffer=new byte[20];
        for(int i=0; i < 5; i++)
            out.write(buffer);
        assert out.position() == 120;
    }

    public void testExpanding2() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1);
        out.write(new byte[]{'b', 'e', 'l', 'a'});

        out=new ByteBufferOutputStream(1);
        out.writeBoolean(true);
        out.writeBoolean(false);
        assert out.position() == 2;

        out=new ByteBufferOutputStream(1);
        out.writeShort(22);
        out.writeShort(23);
        assert out.position() == 4;

        out=new ByteBufferOutputStream(1);
        out.writeInt(23);
        out.writeInt(24);
        assert out.position() == 8;
    }

    public void testExpanding3() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        out.writeInt(1);
        assert out.position() == 4;
        byte[] buf=new byte[1024];
        out.write(buf);
        System.out.println("out=" + out);
        assert out.position() == 1028;

        buf=new byte[512];
        out.write(buf);
        System.out.println("out = " + out);
        assert out.position() == 1028+512;
    }

    public void testRead() {
        byte[] buf={'b', 'e', 'l', 'a'};
        ByteBufferInputStream  in=new ByteBufferInputStream(ByteBuffer.wrap(buf));
        for(byte b: buf)
            assert b == in.read();
        assert -1 == in.read();

        in=new ByteBufferInputStream(ByteBuffer.wrap(buf, 2, 2));
        assert in.read() == 'l';
        assert in.read() == 'a';
        assert -1 == in.read();
    }

    public void testReadFully() {
        byte[] name="Bela".getBytes();
        ByteBufferInputStream  in=new ByteBufferInputStream(ByteBuffer.wrap(name));
        byte[] buf=new byte[10];
        int read=in.read(buf, 0, buf.length);
        assert read == 4;
        assert in.position() == 4;

        in.buf().position(0);
        try {
            in.readFully(buf, 0, buf.length);
            assert false : "readFully() should have thrown an EOFException";
        }
        catch(Exception eof_ex) {
            System.out.println("got exception as expected: " + eof_ex);
            assert eof_ex instanceof EOFException;
        }
    }

    public void testByte() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(Byte.MAX_VALUE * 2);
        for(short i=Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++)
            out.writeByte(i);

        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(short i=Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            byte read=in.readByte();
            assert i == read;
        }
    }

    public void testUnsignedByte() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(20);
        byte[] bytes={-100, -50, 0, 1, 100, 127};
        for(byte b: bytes)
            out.writeByte(b);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(byte b: bytes) {
            int tmp=in.readUnsignedByte();
            assert tmp == (b & 0xff);
        }
    }

    public void testBoolean() throws Exception {
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeBoolean(true);
        out.writeBoolean(false);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        assert in.readBoolean();
        assert !in.readBoolean();
    }

    public void testShort() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(Short.MAX_VALUE * 4 + 10);
        for(int i=Short.MIN_VALUE; i <= Short.MAX_VALUE; i++)
            out.writeShort(i);

        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(int i=Short.MIN_VALUE; i <= Short.MAX_VALUE; i++) {
            short read=in.readShort();
            assert i == read;
        }
    }

    public void testUnsignedShort() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(20);
        out.writeShort(-22);
        out.writeShort(22);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        short num=in.readShort();
        assert num == -22;
        num=in.readShort();
        assert num == 22;

        in.buf().position(0);
        int val=in.readUnsignedShort();
        assert val == 65514;
        val=in.readUnsignedShort();
        assert val == 22;
    }

    public void testChar() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        for(int i=0; i < 500; i++) {
            int ch='a' + i;
            out.writeChar(ch);
        }
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(int i=0; i < 500; i++) {
            char ch=in.readChar();
            assert ch == 'a' + i;
        }
    }

    public void testInt() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        int[] numbers={Integer.MIN_VALUE, -322649, -500, 0, 1, 100, 322649, Integer.MAX_VALUE};
        for(int i: numbers)
            out.writeInt(i);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(int i: numbers) {
            int num=in.readInt();
            assert num == i;
        }
    }

    public void testLong() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        long[] numbers={Long.MIN_VALUE, -322649, -500, 0, 1, 100, 322649, Long.MAX_VALUE};
        for(long i: numbers)
            out.writeLong(i);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(long i: numbers) {
            long num=in.readLong();
            assert num == i;
        }
    }

    public void testCompressedLong() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        long[] numbers={Long.MIN_VALUE, -500, 0, 1, 100, Long.MAX_VALUE};
        for(long i: numbers)
            Bits.writeLongCompressed(i, out);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(long i: numbers) {
            long num=Bits.readLongCompressed(in);
            assert num == i;
        }
    }

    public void testCompressedLongSequence() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        long[] numbers={0, 1, 100, 322649, Long.MAX_VALUE-1000};
        for(long i: numbers)
            Bits.writeLongSequence(i, i + 100,out);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(long i: numbers) {
            long[] seq={0,0};
            Bits.readLongSequence(in, seq, 0);
            assert Arrays.equals(seq, new long[]{i, i+100});
        }
    }

    public void testFloat() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        float[] numbers={-322649.25f, 100.7531f, 0f, 1f, 2.75f, 3.1425f, 322649f, 322649.75f};
        for(float i: numbers)
            out.writeFloat(i);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(float i: numbers) {
            float num=in.readFloat();
            assert num == i;
        }
    }

    public void testDouble() throws IOException {
        ByteBufferOutputStream out=new ByteBufferOutputStream(1024);
        double[] numbers={-322649.25, 100.7531, 0.0, 1.5, 2.75, 3.1425, 322649, 322649.75};
        for(double i: numbers)
            out.writeDouble(i);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        for(double i: numbers) {
            double num=in.readDouble();
            assert num == i;
        }
    }

    public void testWriteBytes() throws IOException {
        String name="Bela";
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeBytes(name);
        assert out.position() == name.length();
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        byte[] tmp=new byte[name.length()];
        int read=in.read(tmp, 0, tmp.length);
        assert read == name.length();
        assert name.equals(new String(tmp));
    }

    public void testWriteChars() throws IOException {
        String name="Bela";
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeChars(name);
        assert out.position() == name.length() * 2;
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        char[] tmp=new char[name.length()];
        for(int i=0; i < name.length(); i++)
            tmp[i]=in.readChar();
        assert name.equals(new String(tmp));

    }

    public void testUTF() throws IOException {
        String name="Bela";
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeUTF(name);
        assert out.position() == name.length() + 2;
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        String tmp=in.readUTF();
        assert name.equals(tmp);
    }

    public void testUTFWithDoubleByteChar() throws IOException {
        String name="Bela\234";
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().flip());
        String tmp=in.readUTF();
        assert name.equals(tmp);
    }

    public void testUTFWithDoubleByteChar2() throws IOException {
        String name="Bela\420";
        ByteBufferOutputStream out=new ByteBufferOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        ByteBufferInputStream in=new ByteBufferInputStream(out.buf().duplicate().flip());
        String tmp=in.readUTF();
        assert name.equals(tmp);

        name="";
        out=new ByteBufferOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        in=new ByteBufferInputStream(out.buf().duplicate().flip());
        tmp=in.readUTF();
        assert name.equals(tmp);

        name=null;
        out=new ByteBufferOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        in=new ByteBufferInputStream(out.buf().duplicate().flip());
        tmp=in.readUTF();
        assert tmp == null;
    }

    public void testReadLine() throws IOException {
        String str="Gallia est omnis divisa in partes tres,\nquarum unam incolunt Belgae,\naliam Aquitani," +
          "\ntertiam qui ipsorum lingua Celtae, nostra Galli appellantur";
        byte[] buf=str.getBytes();
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf));
        List<String> lines=new ArrayList<>(4);
        String line;
        while((line=in.readLine()) !=  null) {
            System.out.println(line);
            lines.add(line);
        }
        assert lines.size() == 4;
    }

    public void testMarkAndReset() {
        byte[] buf="Bela".getBytes();
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf));
        assert in.markSupported();
        assert in.read() == 'B';
        in.mark(0);
        assert in.read() == 'e';
        assert in.read() == 'l';
        in.reset();
        assert in.read() == 'e';
    }

    public void testMarkAndResetWithOffset() {
        byte[] buf="hello world".getBytes();
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf, 6, 5));
        assert in.read() == 'w';
        in.mark(0);
        assert in.read() == 'o';
        assert in.read() == 'r';
        in.reset();
        assert in.read() == 'o';
    }

    public void testResetWithoutMark() {
        byte[] buf="Bela".getBytes();
        ByteBufferInputStream in=new ByteBufferInputStream(ByteBuffer.wrap(buf));
        // We don't throw an exception if no mark
        in.reset();
        assert in.read() == 'B';
    }
}
