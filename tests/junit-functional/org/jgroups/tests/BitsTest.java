package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Bits;
import org.testng.annotations.Test;

import java.io.*;

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

    public void testWriteAndReadInt() throws Exception {
        int[] values={Integer.MIN_VALUE, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, Short.MAX_VALUE, 322649, Integer.MAX_VALUE};
        for(int val: values) {
            byte[] buf=marshall(val);
            int new_val=unmarshalInt(buf);
            System.out.println(val + " --> " + new_val);
            assert val == new_val;
            int size=Bits.size(val);
            assert size == buf.length;
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


    public static void testWriteAndReadLongSequence() throws Exception {
        long[] numbers={0, 1, 50, 127, 128, 254, 255, 256,
          Short.MAX_VALUE, Short.MAX_VALUE +1, Short.MAX_VALUE *2, Short.MAX_VALUE *2 +1,
          100000, 500000, 100000,
          Integer.MAX_VALUE, (long)Integer.MAX_VALUE +1, (long)Integer.MAX_VALUE *2, (long)Integer.MAX_VALUE +10,
          Long.MAX_VALUE /10, Long.MAX_VALUE -1, Long.MAX_VALUE};

        for(long num: numbers) {
            long second_num=num;
            byte[] buf=marshall(new long[]{num, second_num});
            int size=buf.length;
            long[] result=unmarshalLongSeq(buf);
            System.out.println(num + " | " + second_num + " encoded to " + buf.length +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
            assert size == buf.length;

            if(num >= Long.MAX_VALUE || num +1 >= Long.MAX_VALUE)
                continue;
            second_num=num+100;

            buf=marshall(new long[]{num, second_num});
            result=unmarshalLongSeq(buf);
            System.out.println(num + " | " + second_num + " encoded to " + buf.length +
                                 " bytes, decoded to " + result[0] + " | " + result[1]);
            assert num == result[0] && second_num == result[1];
            size=buf.length;
            assert size == buf.length;
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
        Bits.writeLong(val, out);
        return output.toByteArray();
    }

    protected static byte[] marshall(int val) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeInt(val, out);
        return output.toByteArray();
    }

    protected static byte[] marshall(long[] vals) throws Exception {
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutput out=new DataOutputStream(output);
        Bits.writeLongSequence(vals[0], vals[1], out);
        return output.toByteArray();
    }

    protected static long unmarshal(byte[] buf) throws Exception {
        return Bits.readLong(createInputStream(buf));
    }

    protected static int unmarshalInt(byte[] buf) throws Exception {
        return Bits.readInt(createInputStream(buf));
    }

    protected static long[] unmarshalLongSeq(byte[] buf) throws Exception {
        return Bits.readLongSequence(createInputStream(buf));
    }

    protected static DataInput createInputStream(byte [] buf) {
        ByteArrayInputStream input=new ByteArrayInputStream(buf);
        return new DataInputStream(input);
    }


}
