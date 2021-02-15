package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.NioMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Test {@link org.jgroups.NioMessage}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class NioMessageTest extends MessageTestBase {
    protected static final byte[]     ARRAY="hello world".getBytes();
    protected static final ByteBuffer HEAP_BUF=ByteBuffer.wrap("hello world".getBytes());
    protected static final ByteBuffer DIRECT_BUF=ByteBuffer.allocateDirect(ARRAY.length).put(ARRAY).flip();


    public void testConstructor() {
        NioMessage msg=new NioMessage();
        assert msg.getType() == Message.NIO_MSG;
        assert !msg.hasPayload();
        assert !msg.hasArray();
        assert msg.getLength() == 0;
        assert !msg.isDirect();
    }

    public void testConstructor2() {
        NioMessage msg=new NioMessage(null, HEAP_BUF);
        assert msg.getType() == Message.NIO_MSG;
        assert msg.hasPayload();
        assert msg.hasArray();
        assert msg.getLength() > 0;
        assert !msg.isDirect();
    }

    public void testConstructor2Direct() {
        NioMessage msg=new NioMessage(null, DIRECT_BUF);
        assert msg.isDirect();
        assert msg.hasPayload();
        assert !msg.hasArray();
        assert msg.getLength() == ARRAY.length;
    }

    public void testConstructorWithDirectByteBuffer() {
        NioMessage msg=new NioMessage(null, ByteBuffer.allocateDirect(4));
        assert msg.isDirect();
        assert msg.hasPayload();
        assert !msg.hasArray();
        assert msg.getOffset() == 0 && msg.getLength() == 4;
    }

    public void testUseDirectMemory() {
        NioMessage msg=new NioMessage(null).useDirectMemory(true);
        assert !msg.hasArray();
        assert !msg.isDirect();
        msg.setArray(ARRAY, 6, 5);
        assert msg.isDirect();
        assert !msg.hasArray();
        assert msg.getLength() == 5 && msg.getOffset() == 0;
        String s=new String(msg.getArray(), msg.getOffset(), msg.getLength());
        assert s.equals("world");
    }

    public void testGetArray() {
        byte[] array="hello world".getBytes();
        Message msg=new NioMessage(null, HEAP_BUF);
        assert msg.hasArray() && msg.getArray().length == array.length;
    }

    public void testGetArrayDirect() {
        NioMessage msg=new NioMessage(null, DIRECT_BUF);
        assert msg.getArray().length == ARRAY.length;
    }

    public void testSetArrayWithOffset() {
        Message msg=new NioMessage(null);
        byte[] array="hello world".getBytes();
        msg.setArray(array, 6, 5);
        assert msg.getLength() == 5 && msg.getOffset() == 6;
        String s=new String(msg.getArray(), msg.getOffset(), msg.getLength());
        assert s.equals("world");
    }

    public void testSetArrayWithOffsetDirect() {
        NioMessage msg=new NioMessage(null).useDirectMemory(true)
          .setArray(ARRAY, 6, 5).useDirectMemory(false);
        assert msg.getLength() == 5 && msg.getOffset() == 0;
        String s=new String(msg.getArray(), msg.getOffset(), msg.getLength());
        assert s.equals("world");
    }

    public void testSetObject() throws Exception {
        Person person=new Person(53, "Bela");
        Message msg=new NioMessage(null).setObject(person);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(NioMessage.class, buf);
        Person p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testSetObjectDirect() throws Exception {
        Person person=new Person(53, "Bela");
        NioMessage msg=new NioMessage(null).useDirectMemory(true).setObject(person).useDirectMemory(false);
        _testSize(msg);
        byte[] buf=marshal(msg);
        ByteBuffer tmp=ByteBuffer.wrap(buf);
        Message msg2=unmarshal(NioMessage.class, tmp);
        Person p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testSetObject2() throws Exception {
        BasePerson person=new BasePerson(53, "Bela");
        Message msg=new NioMessage(null).setObject(person);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(NioMessage.class, buf);
        BasePerson p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testSetObject2Direct() throws Exception {
        BasePerson person=new BasePerson(53, "Bela");
        Message msg=new NioMessage(null).useDirectMemory(true).setObject(person).useDirectMemory(false);
        _testSize(msg);
        byte[] buf=marshal(msg);
        ByteBuffer tmp=ByteBuffer.wrap(buf);
        Message msg2=unmarshal(NioMessage.class, tmp);
        BasePerson p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testSetObject3() throws Exception {
        Message msg=new NioMessage(null).setObject("hello world");
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(NioMessage.class, buf);
        String s=msg2.getObject();
        assert Objects.equals(s, "hello world");
    }

    public void testSetObject3Direct() throws Exception {
        NioMessage msg=new NioMessage(null).useDirectMemory(true).setObject("hello world").useDirectMemory(false);
        _testSize(msg);
        byte[] buf=marshal(msg);
        ByteBuffer tmp=ByteBuffer.wrap(buf);
        Message msg2=unmarshal(NioMessage.class, tmp);
        String s=msg2.getObject();
        assert Objects.equals(s, "hello world");
    }

    public void testSetNullObject() throws Exception {
        Message msg=new NioMessage(null, null);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(NioMessage.class, buf);
        Object p=msg2.getObject();
        assert p == null;
    }

    public void testSetNullObjectDirect() throws Exception {
        Message msg=new NioMessage(null).useDirectMemory(true)
          .setObject(null).useDirectMemory(false);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(NioMessage.class, buf);
        Object p=msg2.getObject();
        assert p == null;
    }

    public void testSetObjectWithArray() {
        String s1="Bela Ban";
        Message m1=new NioMessage(null).setObject(s1);
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(m1.getArray().length, m1.getLength());
        String s2=m1.getObject();
        Assert.assertEquals(s2, s1);
    }

    public void testSerializableFlag() {
        Message msg=new NioMessage();
        assert msg.isFlagSet(Message.Flag.SERIALIZED) == false;
        msg.setObject("hello");
        assert msg.isFlagSet(Message.Flag.SERIALIZED);
        msg.setObject("hello".getBytes());
        assert msg.isFlagSet(Message.Flag.SERIALIZED) == false;
    }

    public void testSetObjectWithByteBuffer() {
        byte[] H="bela".getBytes();
        Message msg=new NioMessage(null).setObject("bela".getBytes());
        byte[] tmp=msg.getObject();
        assert tmp != null && new String(tmp).equals("bela");

        ByteBuffer buf=ByteBuffer.wrap(H);
        msg.setObject(buf);
        assert msg.getLength() == H.length;
        assert msg.getOffset() == 0;
        byte[] pl=msg.getPayload();
        assert Arrays.equals(pl, H);

        buf=ByteBuffer.allocateDirect(H.length);
        buf.put(H).rewind();

        msg.setObject(buf);
        assert msg.getLength() == H.length;
        assert msg.getOffset() == 0;
        pl=msg.getPayload();
        assert Arrays.equals(pl, H);
    }

    public void testSetNullObject2() {
        Message msg=new NioMessage(null).setObject(null);
        Object obj=msg.getObject();
        assert obj == null;
        msg.setObject(322649);
        obj=msg.getObject();
        assert obj.equals(322649);
        msg.setObject(null);
        obj=msg.getObject();
        assert obj == null;
    }

}
