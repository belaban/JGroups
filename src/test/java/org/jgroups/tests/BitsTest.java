package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AsciiString;
import org.jgroups.util.Bits;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Tests the methods of {@link org.jgroups.util.Bits}
 * @author Bela Ban
 * @author Sanne Grinovero
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class BitsTest {

    public void testWriteAndReadLong() throws Exception {
        long[] values={Long.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, Short.MAX_VALUE, Integer.MIN_VALUE, 322649,
          Integer.MAX_VALUE, (long)Integer.MAX_VALUE + 100, Long.MAX_VALUE - 10, Long.MAX_VALUE};
        for(long val: values) {
            byte[] buf=marshall(val);
            long new_val=unmarshal(buf);
            System.out.println(val + " --> " + new_val);
            assert val == new_val;
            int size=Bits.size(val);
            assert size == buf.length;
        }
    }

    public void testWriteAndReadLongByteBuffer() throws Exception {
        long[] values={Long.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, Short.MAX_VALUE, Integer.MIN_VALUE, 322649,
          Integer.MAX_VALUE, (long)Integer.MAX_VALUE + 100, Long.MAX_VALUE - 10, Long.MAX_VALUE};
        for(long val: values) {
            ByteBuffer buf=marshallToByteBuffer(val);
            buf.rewind();
            long new_val=unmarshal(buf);
            System.out.println(val + " --> " + new_val);
            assert val == new_val;
            int size=Bits.size(val);
            assert size == buf.capacity();
        }
    }

    public void testWriteAndReadInt() throws Exception {
        int[] values={Integer.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, Short.MAX_VALUE, 322649, Integer.MAX_VALUE};
        for(int val: values) {
            byte[] buf=marshallInt(val);
            int new_val=unmarshalInt(buf);
            System.out.println(val + " --> " + new_val);
            assert val == new_val;
            int size=Bits.size(val);
            assert size == buf.length;
        }
    }

    public void testWriteAndReadIntByteBuffer() throws Exception {
        int[] values={Integer.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, Short.MAX_VALUE, 322649, Integer.MAX_VALUE};
        for(int val: values) {
            ByteBuffer buf=marshallToByteBuffer(val);
            buf.rewind();
            int new_val=unmarshalInt(buf);
            System.out.println(val + " --> " + new_val);
            assert val == new_val;
            int size=Bits.size(val);
            assert size == buf.capacity();
        }
    }
    

    public static void testSizeLong() {
        int[] shifts={0, 1, 2, 4, 7, 8, 15, 16, 17, 23, 24, 25, 31, 32, 33, 39, 40, 41, 47, 48, 49, 55, 56};
        assert Bits.size((long)0) == 1;
        for(int shift: shifts) {
            long num=((long)1) << shift;
            int size=Bits.size(num);
            System.out.println(num + " needs " + size + " bytes");
            int num_bytes_required=(shift / 8) +2;
            assert size == num_bytes_required;
        }
    }

    public static void testSizeInt() {
        int[] shifts={0, 1, 2, 4, 7, 8, 15, 16, 17, 23, 24};
        assert Bits.size(0) == 1;
        for(int shift: shifts) {
            int num=1 << shift;
            int size=Bits.size(num);
            System.out.println(num + " needs " + size + " bytes");
            int num_bytes_required=(shift / 8) +2;
            assert size == num_bytes_required;
        }
    }


    public void testWriteAndReadLongSequence() throws Exception {
        long[] numbers={0, 1, 50, 127, 128, 254, 255, 256,
          Short.MAX_VALUE, Short.MAX_VALUE +1, Short.MAX_VALUE *2, Short.MAX_VALUE *2 +1,
          100000, 500000, 100000,
          Integer.MAX_VALUE, (long)Integer.MAX_VALUE +1, (long)Integer.MAX_VALUE *2, (long)Integer.MAX_VALUE +10,
          Long.MAX_VALUE /10, Long.MAX_VALUE -1, Long.MAX_VALUE};

        for(long num: numbers) {
            long second_num=num;
            byte[] buf=marshall(num, second_num);
            int size=buf.length;
            long[] result={0,0};
            unmarshalLongSeq(buf, result, 0);
            System.out.println(num + " | " + second_num + " encoded to " + buf.length +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
            assert size == buf.length;

            if(num >= Long.MAX_VALUE || num +1 >= Long.MAX_VALUE)
                continue;
            second_num=num+100;

            buf=marshall(num, second_num);
            result=new long[]{0,0};
            unmarshalLongSeq(buf, result, 0);
            System.out.println(num + " | " + second_num + " encoded to " + buf.length +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
            size=buf.length;
            assert size == buf.length;
        }
    }


    public void testWriteAndReadLongSequenceByteBuffer() throws Exception {
        long[] numbers={0, 1, 50, 127, 128, 254, 255, 256,
          Short.MAX_VALUE, Short.MAX_VALUE +1, Short.MAX_VALUE *2, Short.MAX_VALUE *2 +1,
          100000, 500000, 100000,
          Integer.MAX_VALUE, (long)Integer.MAX_VALUE +1, (long)Integer.MAX_VALUE *2, (long)Integer.MAX_VALUE +10,
          Long.MAX_VALUE /10, Long.MAX_VALUE -1, Long.MAX_VALUE};

        for(long num: numbers) {
            long second_num=num;
            ByteBuffer buf=marshallToByteBuffer(num, second_num);
            int size=buf.limit();
            buf.rewind();
            long[] result={0,0};
            unmarshalLongSeq(buf, result);
            System.out.println(num + " | " + second_num + " encoded to " + size +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
            assert size == buf.limit();

            if(num >= Long.MAX_VALUE || num +1 >= Long.MAX_VALUE)
                continue;
            second_num=num+100;

            buf=marshallToByteBuffer(num, second_num);
            buf.rewind();
            result=new long[]{0,0};
            unmarshalLongSeq(buf, result);
            System.out.println(num + " | " + second_num + " encoded to " + buf.limit() +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
        }
    }


    public void testAsciiString() throws Exception {
        AsciiString[] strings={null, new AsciiString("hello world"), new AsciiString("1")};
        for(AsciiString str: strings) {
            byte[] buf=marshall(str);
            AsciiString s=unmarshallAsciiString(buf);
            assert str == null && s == null || str.equals(s);
        }
    }

    public void testAsciiStringByteBuffer() throws Exception {
        AsciiString[] strings={null, new AsciiString("hello world"), new AsciiString("1")};
        for(AsciiString str: strings) {
            ByteBuffer buf=marshallToByteBuffer(str);
            buf.rewind();
            AsciiString s=unmarshallAsciiString(buf);
            assert str == null && s == null || str.equals(s);
        }
    }

    public void testString() throws Exception {
        String[] strings={null, "hello world", "1"};
        for(String str: strings) {
            byte[] buf=marshall(str);
            String s=unmarshallString(buf);
            assert str == null && s == null || str.equals(s);
        }
    }

    public void testStringByteBuffer() throws Exception {
        String[] strings={null, "hello world", "1"};
        for(String str: strings) {
            ByteBuffer buf=marshallToByteBuffer(str, false);
            buf.rewind();
            String s=unmarshallString(buf);
            assert str == null && s == null || str.equals(s);

            buf=marshallToByteBuffer(str, true);
            buf.rewind();
            s=unmarshallString(buf);
            assert str == null && s == null || str.equals(s);
        }
    }

    public void testFloat() throws Exception {
        float[] floats={-3.421f, 0.0f, 3.14f};
        for(float f: floats) {
            byte[] buf=marshal(f);
            float fl=unmarshalFloat(buf);
            assert fl == f;
        }
    }

    public void testFloatByteBuffer() throws Exception {
        float[] floats={-3.421f, 0.0f, 0.001f, 3.14f, 32649.6352f};
        for(float f: floats) {
            ByteBuffer buf=marshalToByteBuffer(f);
            buf.rewind();
            float fl=unmarshalFloat(buf);
            System.out.printf("fl=%f f=%f\n", fl, f);
            assert fl == f;
        }
    }

    public void testDouble() throws Exception {
        double[] doubles={-3.421, 0.0, 3.14, 0.001, 322649.26635};
        for(double d: doubles) {
            byte[] buf=marshal(d);
            double dbl=unmarshalDouble(buf);
            assert dbl == d;
        }
    }

    public void testDoubleByteBuffer() throws Exception {
        double[] doubles={-3.421, 0.0, 3.14, 0.001, 322649.26635};
        for(double d: doubles) {
            ByteBuffer buf=marshalToByteBuffer(d);
            buf.rewind();
            double dbl=unmarshalDouble(buf);
            assert dbl == d;
        }
    }

    public void testChar() {
        char[] chars={'b',
          '\u00fc', // u umlaut,
          '\u00dc', // U umlaut,
          '\u00eb'}; // e umlaut;
        for(char c: chars) {
            byte[] buf=new byte[Character.BYTES];
            Bits.writeChar(c, buf, 0);
            char cc=Bits.readChar(buf, 0);
            assert c == cc;
            ByteBuffer bb=ByteBuffer.wrap(buf);
            cc=Bits.readChar(bb);
            assert cc == c;
        }
    }

    public void testShort() {
        short[] shorts={0,-100,200, Short.MIN_VALUE, Short.MAX_VALUE};
        for(short s: shorts) {
            byte[] buf=new byte[Short.BYTES];
            Bits.writeShort(s, buf, 0);
            short ss=Bits.readShort(buf, 0);
            assert s == ss;
            ByteBuffer bb=ByteBuffer.wrap(buf);
            ss=Bits.readShort(bb);
            assert s == ss;
        }
    }

    public void testInt() {
        int[] ints={0,-100,200, Integer.MIN_VALUE, Integer.MAX_VALUE};
        for(int i: ints) {
            byte[] buf=new byte[Integer.BYTES];
            Bits.writeInt(i, buf, 0);
            int ii=Bits.readInt(buf, 0);
            assert i == ii;

            ByteBuffer bb=ByteBuffer.wrap(buf);
            ii=Bits.readInt(bb);
            assert i == ii;
        }
    }

    public void testIntCompressed() throws Exception {
        int[] ints={0, 200, 322649, Integer.MIN_VALUE, Integer.MAX_VALUE};
        for(int i: ints) {
            byte[] buf=new byte[Integer.BYTES+1];

            Bits.writeIntCompressed(i, buf, 0);
            int ii=Bits.readIntCompressed(buf, 0);
            assert i == ii;

            ByteBuffer bb=ByteBuffer.allocate(Integer.BYTES+1);
            Bits.writeIntCompressed(i, bb);
            bb.flip();
            ii=Bits.readIntCompressed(bb);
            assert i == ii;

            buf=marshallInt(i);
            ii=unmarshalInt(buf);
            assert i == ii;
            int size=Bits.size(i);
            assert buf.length == size;
        }
    }


    public void testLong() {
        long[] values={0,-100, 200L, 322649L, Long.MIN_VALUE, Long.MAX_VALUE};
        for(long i: values) {
            byte[] buf=new byte[Long.BYTES];
            Bits.writeLong(i, buf, 0);
            long ii=Bits.readLong(buf, 0);
            assert i == ii;

            ByteBuffer bb=ByteBuffer.wrap(buf);
            ii=Bits.readLong(bb);
            assert i == ii;
        }
    }

    public void testLongCompressed() throws Exception {
        long[] values={0, -100L, 200L, 322649L, Long.MIN_VALUE, Long.MAX_VALUE};
        for(long i: values) {
            byte[] buf=new byte[Long.BYTES+1];

            Bits.writeLongCompressed(i, buf, 0);
            long ii=Bits.readLongCompressed(buf, 0);
            assert i == ii;

            ByteBuffer bb=ByteBuffer.allocate(Long.BYTES+1);
            Bits.writeLongCompressed(i, bb);
            bb.flip();
            ii=Bits.readLongCompressed(bb);
            assert i == ii;

            buf=marshall(i);
            ii=unmarshalLong(buf);
            assert i == ii;
            int size=Bits.size(i);
            assert buf.length == size;
        }
    }


    protected static String printBuffer(byte[] buf) {
        StringBuilder sb=new StringBuilder();
        if(buf != null) {
            for(byte b: buf)
                sb.append(b).append(" ");
        }
        return sb.toString();
    }

    protected static byte[] marshall(long val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeLongCompressed(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshallToByteBuffer(long val) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeLongCompressed(val, buf);
        return buf;
    }

    protected static byte[] marshallInt(int val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeIntCompressed(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshallToByteBuffer(int val) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeIntCompressed(val, buf);
        return buf;
    }

    protected static byte[] marshall(long val1, long val2) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeLongSequence(val1, val2, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshallToByteBuffer(long val1, long val2) throws Exception {
        int size=Bits.size(val1, val2);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeLongSequence(val1, val2, buf);
        return buf;
    }

    protected static byte[] marshall(AsciiString val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeAsciiString(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshallToByteBuffer(AsciiString val) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeAsciiString(val, buf);
        return buf;
    }

    protected static byte[] marshall(String val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeString(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshallToByteBuffer(String val, boolean direct) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=direct? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
        Bits.writeString(val, buf);
        return buf;
    }

    protected static byte[] marshal(float val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeFloat(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshalToByteBuffer(float val) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeFloat(val, buf);
        return buf;
    }

    protected static byte[] marshal(double val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeDouble(val, out);
        return output.toByteArray();
    }

    protected static ByteBuffer marshalToByteBuffer(double val) throws Exception {
        int size=Bits.size(val);
        ByteBuffer buf=ByteBuffer.allocate(size);
        Bits.writeDouble(val, buf);
        return buf;
    }

    protected static long unmarshal(byte[] buf) throws Exception {
        return Bits.readLongCompressed(createInputStream(buf));
    }

    protected static long unmarshal(ByteBuffer buf) throws Exception {
        return Bits.readLongCompressed(buf);
    }

    protected static int unmarshalInt(byte[] buf) throws Exception {
        return Bits.readIntCompressed(createInputStream(buf));
    }

    protected static long unmarshalLong(byte[] buf) throws Exception {
        return Bits.readLongCompressed(createInputStream(buf));
    }

    protected static int unmarshalInt(ByteBuffer buf) throws Exception {
        return Bits.readIntCompressed(buf);
    }

    protected static void unmarshalLongSeq(byte[] buf, long[] seqnos, int index) throws Exception {
        Bits.readLongSequence(createInputStream(buf), seqnos, index);
    }

    protected static void unmarshalLongSeq(ByteBuffer buf, long[] seqnos) throws Exception {
        Bits.readLongSequence(buf, seqnos);
    }

    protected static AsciiString unmarshallAsciiString(byte[] buf) throws Exception {
        return Bits.readAsciiString(createInputStream(buf));
    }

    protected static AsciiString unmarshallAsciiString(ByteBuffer buf) throws Exception {
        return Bits.readAsciiString(buf);
    }

    protected static String unmarshallString(byte[] buf) throws Exception {
        return Bits.readString(createInputStream(buf));
    }

    protected static String unmarshallString(ByteBuffer buf) throws Exception {
        return Bits.readString(buf);
    }

    protected static float unmarshalFloat(byte[] buf) throws Exception {
        return Bits.readFloat(createInputStream(buf));
    }

    protected static float unmarshalFloat(ByteBuffer buf) throws Exception {
        return Bits.readFloat(buf);
    }

    protected static double unmarshalDouble(byte[] buf) throws Exception {
        return Bits.readDouble(createInputStream(buf));
    }

    protected static double unmarshalDouble(ByteBuffer buf) throws Exception {
        return Bits.readDouble(buf);
    }


    protected static DataInput createInputStream(byte [] buf) {
        ByteArrayInputStream input=new ByteArrayInputStream(buf);
        return new DataInputStream(input);
    }


}
