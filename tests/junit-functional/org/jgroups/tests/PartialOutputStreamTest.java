package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests {@link org.jgroups.util.PartialOutputStream}
 * @author Bela Ban
 * @since  5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class PartialOutputStreamTest {
    protected static final byte[] array={0,1,2,3,4,5,6,7,8,9,10};

    public void testRemaining() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 1);
        assert pos.remaining() == 1;
        pos.write('A');
        assert pos.remaining() == 0;
        pos=new PartialOutputStream(out, 4, 5);
        assert pos.remaining() == 5;
        pos.write("bela".getBytes());
        assert pos.remaining() == 5;
        pos.write("Hello".getBytes());
        assert pos.remaining() == 0;
    }

    public void testWriteByteInRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 1);
        assert pos.remaining() == 1;
        pos.write('A');
        assert out.position() == 1 && pos.position() == 1 && out.buffer()[0] == 'A';
        assert pos.remaining() == 0;
    }

    public void testWriteByteBelowRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 2, 1);
        assert pos.remaining() == 1;
        pos.write('A');
        assert pos.position() == 1 && out.position() == 0;
        assert pos.remaining() == 1;

        pos.write('B');
        assert pos.position() == 2 && out.position() == 0;
        assert pos.remaining() == 1;

        pos.write('C');
        assert pos.position() == 3 && out.position() == 1;
        assert out.buffer()[0] == 'C';
        assert pos.remaining() == 0;
    }

    public void testWriteByteBeyondRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 1);
        pos.write('A');
        assert pos.position() == 1 && out.position() == 1;
        assert out.buffer()[0] == 'A';

        pos.write('B');
        assert pos.position() == 1 && out.position() == 1;

        pos.write('C');
        assert pos.position() == 1 && out.position() == 1;
    }


    public void testWriteByteArrayInRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 16);
        pos.write(array);
        assert out.position() == array.length && pos.position() == array.length;
        assert pos.remaining() == 5;
        compare(out.buffer(), array, array.length);
    }


    public void testWriteByteArrayBeyondRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 8);
        pos.write(array);
        assert out.position() == 8 && pos.position() == 8;
        assert pos.remaining() == 0;
        pos.write('A');
        assert out.position() == 8 && pos.position() == 8;
        assert pos.remaining() == 0;
    }


    public void testWriteByteArrayBelowRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 16, 20);
        pos.write(array, 0, array.length);
        assert out.position() == 0 && pos.position() == array.length;
        assert pos.remaining() == 20;
        byte[] tmp={10,11,12,13,14};
        pos.write(tmp, 0, tmp.length);
        assert out.position() == 0 && pos.position() == 16;
        assert pos.remaining() == 20;
        tmp=new byte[]{15,16,17};
        pos.write(tmp, 0, tmp.length);
        assert out.position() == 3 && pos.position() == 19;
        assert pos.remaining() == 17;
        compare(out.buffer(), tmp, tmp.length);
    }

    public void testWriteByteArrayBeyondAndExtendingIntoRange() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 4, 5);
        assert pos.remaining() == 5;
        pos.write(array, 0, array.length);
        assert out.position() == 5 && pos.position() == 9;
        assert pos.remaining() == 0;
        compare(out.buffer(), Arrays.copyOfRange(array, 4, 9), 5);
    }

    public void testWriteByteArrayBeyondAndExtendingIntoRange2() {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        PartialOutputStream pos=new PartialOutputStream(out, 0, 5);
        assert pos.remaining() == 5;
        pos.write(array, 0, array.length);
        assert out.position() == 5 && pos.position() == 5;
        assert pos.remaining() == 0;
        compare(out.buffer(), Arrays.copyOfRange(array, 0, 5), 5);
    }


    public void testFragmentationWithSerializableObject() throws Exception {
        MessageSendTest.MyData data=new MessageSendTest.MyData(1, 1200);
        Message msg=new ObjectMessage(null, data);

        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(1300);
        Range[] fragments={new Range(0, 500), new Range(500,500), new Range(1000, 300)};

        for(Range r: fragments) {
            PartialOutputStream pos=new PartialOutputStream(out, (int)r.low, (int)r.high);
            msg.writeTo(pos);
        }

        assert out.position() <= 1300
          : String.format("we should not expand the initial capacity (1300): capacity is %d\n", out.position());

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        ObjectMessage msg2=new ObjectMessage();
        msg2.readFrom(in);
        MessageSendTest.MyData obj=msg2.getObject();
        System.out.printf("re-read obj: %s\n", obj);
        assert obj.num == data.num;
        assert Arrays.equals(obj.data, data.data);
    }

    public void testFragmentationWithSizeStreamableObject() throws Exception {
        MessageSendTest.MySizeData data=new MessageSendTest.MySizeData(1, 1200);
        Message msg=new ObjectMessage(null, data);

        int size=data.serializedSize();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size);
        Range[] fragments={new Range(0, 500), new Range(500,500), new Range(1000, 300)};

        for(Range r: fragments) {
            PartialOutputStream pos=new PartialOutputStream(out, (int)r.low, (int)r.high);
            msg.writeTo(pos);
        }

        assert out.position() <= 1300
          : String.format("we should not expand the initial capacity (1300): capacity is %d\n", out.position());

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        ObjectMessage msg2=new ObjectMessage();
        msg2.readFrom(in);
        MessageSendTest.MySizeData obj=msg2.getObject();
        System.out.printf("re-read obj: %s\n", obj);
        assert obj.num == data.num;
        assert Arrays.equals(obj.data, data.data);
    }

    protected static void compare(byte[] b1, byte[] b2, int len) {
        for(int i=0; i < len; i++)
            assert b1[i] == b2[i] : String.format("%s != %s\n", Util.byteArrayToHexString(b1), Util.byteArrayToHexString(b2));
        System.out.printf("%s == %s\n", Util.byteArrayToHexString(b1, 0, len), Util.byteArrayToHexString(b2, 0, len));
    }

}
