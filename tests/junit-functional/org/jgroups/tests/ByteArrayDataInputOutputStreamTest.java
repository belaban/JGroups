package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ByteArrayDataInputStream} and {@link ByteArrayDataOutputStream}.
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class ByteArrayDataInputOutputStreamTest {

    public void testConstructors() throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(32);
        byte[] tmp="hello world".getBytes();
        out.writeInt(tmp.length);
        out.write(tmp);
        ByteBuffer input_buf=ByteBuffer.wrap(out.buffer(), 0, out.position());

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(input_buf);
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
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);
        assert in.position() == 0;
        assert in.limit() == buf.length;
        assert in.capacity() == buf.length;

        in=new ByteArrayDataInputStream(buf, 0, 4);
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
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, 10, 4);
        assert in.position() == 10;
        assert in.limit() == 14;
        assert in.capacity() == buf.length;
        byte[] tmp=new byte[4];
        in.read(tmp, 0, tmp.length);
        assert "Bela".equals(new String(tmp));
    }

    public void testOffsetAndLimit2() {
        final byte[] data={'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(4);
        out.write(data, 6, 4);
        byte[] tmp=Arrays.copyOfRange(data, 6, 10);
        assert Arrays.equals(tmp, out.buffer());
    }

    public void testReadBeyondLimit() {
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

        in.position(10);
        for(int i=0; i < 4; i++) in.read();
        assert in.read() == -1;
    }


    public void testPosition() {
        byte[] buf=new byte[100];
        buf[10]='B'; buf[11]='e'; buf[12]='l'; buf[13]='a';
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, 10, 4);
        in.position(13);
        try {
            in.position(14);
        }
        catch(Exception ex) {
            System.out.println("caught exception as expected: " + ex);
            assert ex instanceof IndexOutOfBoundsException;
        }
    }

    public void testExpanding() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1);
        for(int i='A'; i < 'A' + 20; i++)
            out.write(i);
        assert out.position() == 20;

        byte[] buffer=new byte[20];
        for(int i=0; i < 5; i++)
            out.write(buffer);
        assert out.position() == 120;
    }

    public void testExpanding2() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1);
        out.write(new byte[]{'b', 'e', 'l', 'a'});

        out=new ByteArrayDataOutputStream(1);
        out.writeBoolean(true);
        out.writeBoolean(false);
        assert out.position() == 2;

        out=new ByteArrayDataOutputStream(1);
        out.writeShort(22);
        out.writeShort(23);
        assert out.position() == 4;

        out=new ByteArrayDataOutputStream(1);
        out.writeInt(23);
        out.writeInt(24);
        assert out.position() == 8;
    }

    public void testExpanding3() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
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

    public void testSkipBytes() {
        byte[] buf={'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'};
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);
        assert in.position() == 0;
        int skipped=in.skipBytes(6);
        assert skipped == 6;
        assert in.position() == 6;

        skipped=in.skipBytes(0);
        assert skipped == 0;
        assert in.position() == 6;

        skipped=in.skipBytes(-1);
        assert skipped == 0;
        assert in.position() == 6;

        skipped=in.skipBytes(5);
        assert skipped == 5;
        assert in.position() == 11;

        in.position(6);
        skipped=in.skipBytes(20);
        assert skipped == 5;
        assert in.position() == 11;
    }

    public void testRead() {
        byte[] buf={'b', 'e', 'l', 'a'};
        ByteArrayDataInputStream  in=new ByteArrayDataInputStream(buf);
        for(byte b: buf)
            assert b == in.read();
        assert -1 == in.read();

        in=new ByteArrayDataInputStream(buf, 2, 2);
        assert in.read() == 'l';
        assert in.read() == 'a';
        assert -1 == in.read();

        ByteBuffer buffer=ByteBuffer.wrap(buf);
        System.out.println("buffer = " + buffer);
    }

    public void testReadFully() {
        byte[] name="Bela".getBytes();
        ByteArrayDataInputStream  in=new ByteArrayDataInputStream(name);
        byte[] buf=new byte[10];
        int read=in.read(buf, 0, buf.length);
        assert read == 4;
        assert in.position() == 4;

        in.position(0);
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
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(Byte.MAX_VALUE * 2);
        for(short i=Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++)
            out.writeByte(i);

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(short i=Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
            byte read=in.readByte();
            assert i == read;
        }
    }

    public void testUnsignedByte() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(20);
        byte[] bytes={-100, -50, 0, 1, 100, 127};
        for(byte b: bytes)
            out.writeByte(b);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(byte b: bytes) {
            int tmp=in.readUnsignedByte();
            assert tmp == (b & 0xff);
        }
    }

    public void testBoolean() throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeBoolean(true);
        out.writeBoolean(false);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        assert in.readBoolean();
        assert !in.readBoolean();
    }

    public void testShort() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(Short.MAX_VALUE * 4 + 10);
        for(int i=Short.MIN_VALUE; i <= Short.MAX_VALUE; i++)
            out.writeShort(i);

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(int i=Short.MIN_VALUE; i <= Short.MAX_VALUE; i++) {
            short read=in.readShort();
            assert i == read;
        }
    }

    public void testUnsignedShort() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(20);
        out.writeShort(-22);
        out.writeShort(22);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        short num=in.readShort();
        assert num == -22;
        num=in.readShort();
        assert num == 22;

        in.position(0);
        int val=in.readUnsignedShort();
        assert val == 65514;
        val=in.readUnsignedShort();
        assert val == 22;
    }

    public void testChar() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        for(int i=0; i < 500; i++) {
            int ch='a' + i;
            out.writeChar(ch);
        }
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(int i=0; i < 500; i++) {
            char ch=in.readChar();
            assert ch == 'a' + i;
        }
    }

    public void testInt() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        int[] numbers={Integer.MIN_VALUE, -322649, -500, 0, 1, 100, 322649, Integer.MAX_VALUE};
        for(int i: numbers)
            out.writeInt(i);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(int i: numbers) {
            int num=in.readInt();
            assert num == i;
        }
    }

    public void testLong() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        long[] numbers={Long.MIN_VALUE, -322649, -500, 0, 1, 100, 322649, Long.MAX_VALUE};
        for(long i: numbers)
            out.writeLong(i);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(long i: numbers) {
            long num=in.readLong();
            assert num == i;
        }
    }

    public void testCompressedLong() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        long[] numbers={Long.MIN_VALUE, -500, 0, 1, 100, Long.MAX_VALUE};
        for(long i: numbers)
            Bits.writeLongCompressed(i, out);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(long i: numbers) {
            long num=Bits.readLongCompressed(in);
            assert num == i;
        }
    }

    public void testCompressedLongSequence() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        long[] numbers={0, 1, 100, 322649, Long.MAX_VALUE-1000};
        for(long i: numbers)
            Bits.writeLongSequence(i, i + 100,out);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(long i: numbers) {
            long[] seq={0,0};
            Bits.readLongSequence(in, seq, 0);
            assert Arrays.equals(seq, new long[]{i, i+100});
        }
    }

    public void testFloat() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        float[] numbers={-322649.25f, 100.7531f, 0f, 1f, 2.75f, 3.1425f, 322649f, 322649.75f};
        for(float i: numbers)
            out.writeFloat(i);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(float i: numbers) {
            float num=in.readFloat();
            assert num == i;
        }
    }

    public void testDouble() throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1024);
        double[] numbers={-322649.25, 100.7531, 0.0, 1.5, 2.75, 3.1425, 322649, 322649.75};
        for(double i: numbers)
            out.writeDouble(i);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        for(double i: numbers) {
            double num=in.readDouble();
            assert num == i;
        }
    }


    public void testWriteBytes() {
        String name="Bela";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeBytes(name);
        assert out.position() == name.length();
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        byte[] tmp=new byte[name.length()];
        int read=in.read(tmp, 0, tmp.length);
        assert read == name.length();
        assert name.equals(new String(tmp));
    }


    public void testWriteChars() throws IOException {
        String name="Bela";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeChars(name);
        assert out.position() == name.length() * 2;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        char[] tmp=new char[name.length()];
        for(int i=0; i < name.length(); i++)
            tmp[i]=in.readChar();
        assert name.equals(new String(tmp));

    }

    public void testUTF() throws IOException {
        String name="Bela";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeUTF(name);
        assert out.position() == name.length() + 2;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        String tmp=in.readUTF();
        assert name.equals(tmp);
    }

    public void testUTFWithDoubleByteChar() throws IOException {
        String name="Bela\234";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        String tmp=in.readUTF();
        assert name.equals(tmp);
    }

    public void testUTFWithDoubleByteChar2() throws IOException {
        String name="Bela\420";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer());
        String tmp=in.readUTF();
        assert name.equals(tmp);

        name="";
        out=new ByteArrayDataOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        in=new ByteArrayDataInputStream(out.buffer());
        tmp=in.readUTF();
        assert name.equals(tmp);

        name=null;
        out=new ByteArrayDataOutputStream(2);
        out.writeUTF(name);
        assert out.position() == Bits.sizeUTF(name);
        in=new ByteArrayDataInputStream(out.buffer());
        tmp=in.readUTF();
        assert tmp == null;
    }

    public static void testReadLine() throws IOException {
        String str="Gallia est omnis divisa in partes tres,\n\rquarum unam incolunt Belgae,\naliam Aquitani," +
          "\ntertiam qui ipsorum lingua Celtae, nostra Galli appellantur";
        byte[] buf=str.getBytes();
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);
        List<String> lines=new ArrayList<>(4);
        String line;
        while((line=in.readLine()) !=  null) {
            System.out.println(line);
            lines.add(line);
        }
        assert lines.size() == 4;
    }
}
