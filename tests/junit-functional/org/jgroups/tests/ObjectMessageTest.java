package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.protocols.FRAG4;
import org.jgroups.protocols.Fragmentation;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Objects;

/**
 * Test {@link ObjectMessage}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class ObjectMessageTest extends MessageTestBase {

    public void testConstructor() {
        Message msg=new ObjectMessage();
        assert msg.getType() == Message.OBJ_MSG;
        assert !msg.hasArray();
        assert msg.getLength() == 0;
    }

    public void testConstructor2() {
        Message msg=new ObjectMessage();
        assert msg.getType() == Message.OBJ_MSG;
        assert !msg.hasArray();
        assert msg.getLength() == 0;
    }


    public void testObject() throws Exception {
        Message msg=new ObjectMessage(null, new Person(53, "Bela"));
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        Person p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }


    public void testObject2() throws Exception {
        Message msg=new ObjectMessage(null, new BasePerson(53, "Bela"));
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        BasePerson p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testObject3() throws Exception {
        Message msg=new ObjectMessage(null, "hello world");
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        String s=msg2.getObject();
        assert Objects.equals(s, "hello world");
    }

    public void testObject4() throws Exception {
        Message msg=new ObjectMessage(null, 55);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        int num=msg2.getObject();
        assert num == 55;
    }

    public void testObject5() throws Exception {
        byte[] BUF="hello world".getBytes();
        Message msg=new ObjectMessage(null, BUF);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        byte[] tmp=msg2.getObject();
        assert new String(tmp).equals("hello world");
    }

    public void testMarshalling() throws Exception {
        Object[] objects={null, int.class, Boolean.class, byte[].class, Person.class, "hello".getBytes(), (short)50, 50, 50L,
        3.2345F, 3.1234, "hello world", "B\u00e9la B\u00060n"};
        for(Object obj: objects) {
            Message msg=new ObjectMessage(null, obj);
            _testSize(msg);
            byte[] buf=marshal(msg);
            Message msg2=unmarshal(ObjectMessage.class, buf);
            Object tmp=msg2.getObject();
            if(tmp instanceof byte[] && obj instanceof byte[])
                assert Arrays.equals((byte[])tmp, (byte[])obj);
            else
                assert Objects.equals(tmp, obj) :
                  String.format("%s (%s) != %s (%s)", tmp, tmp != null? tmp.getClass() : "null", obj, obj != null? obj.getClass(): "null");
        }
    }

    public void testSetNullObject() throws Exception {
        Message msg=new ObjectMessage(null, null);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        Person p=msg2.getObject();
        assert p == null;
    }

    public void testSetNullObject2() throws Exception {
        Message msg=new ObjectMessage(null, null);
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        Person p=msg2.getObject();
        assert p == null;
    }

    public void testSetObject() {
        Message msg=new ObjectMessage(null, new Person(53, "Bela"));
        assert msg.getObject() != null;
        msg.setObject(new Person(15, "Nicole"));
        Person p=msg.getObject();
        assert p.age == 15 && p.name.equals("Nicole");
        msg.setObject(null);
        assert msg.getObject() == null;
    }

    public void testSize() {
        Message msg=new ObjectMessage();
        int size=msg.size();
        assert size > 1;
    }


    // https://issues.redhat.com/browse/JGRP-2285
    public void testIncorrectSize() throws Exception {
        try(JChannel a=create("A");
            JChannel b=create("B");) {
            a.connect("test-size");
            b.connect("test-size");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
            IncorrectSizeObject obj=new IncorrectSizeObject(1000);
            MyReceiver<IncorrectSizeObject> r=new MyReceiver<>();
            b.setReceiver(r);
            Message msg=new ObjectMessage(b.getAddress(), obj);
            a.send(msg);
            Util.waitUntil(10000, 250, () -> r.size() > 0);
            IncorrectSizeObject obj2=r.list().get(0);
            assert obj2 != null && obj2.buf.length == 1000;
        }
    }

    // https://issues.redhat.com/browse/JGRP-2289
    public void testIncorrectSizeWithFRAG4() throws Exception {
        try(JChannel a=create("A");
            JChannel b=create("B")) {
            setFRAG4(500, a,b); // replaces any existing fragmentation protocol with FRAG4
            a.connect("test-size");
            b.connect("test-size");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
            IncorrectSizeObject obj=new IncorrectSizeObject(1000);
            MyReceiver<IncorrectSizeObject> r=new MyReceiver<>();
            b.setReceiver(r);
            Message msg=new ObjectMessage(b.getAddress(), obj);
            a.send(msg);
            Util.waitUntil(10000, 250, () -> r.size() > 0);
            IncorrectSizeObject obj2=r.list().get(0);
            assert obj2 != null && obj2.buf.length == 1000;
        }
    }

    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack()).setName(name);
    }

    // removes any existing fragmentation protocols (if any) with FRAG4
    protected static void setFRAG4(int frag_size, JChannel... channels) throws Exception {
        for(JChannel c: channels) {
            ProtocolStack stack=c.getProtocolStack();
            stack.removeProtocols(Fragmentation.class);
            FRAG4 f=new FRAG4().setFragSize(frag_size);
            stack.insertProtocolAtTop(f);
            f.init();
        }
    }


}
