package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
        Message msg=new ObjectMessage(null, new Person(53, "Bela"));
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        Person p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }


    public void testObject3() throws Exception {
        Message msg=new ObjectMessage(null, new BasePerson(53, "Bela"));
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        BasePerson p=msg2.getObject();
        assert p != null && p.name.equals("Bela") && p.age == 53;
    }

    public void testObject4() throws Exception {
        Message msg=new ObjectMessage(null, "hello world");
        _testSize(msg);
        byte[] buf=marshal(msg);
        Message msg2=unmarshal(ObjectMessage.class, buf);
        String s=msg2.getObject();
        assert Objects.equals(s, "hello world");
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


    protected static class BasePerson implements Streamable {
        protected int    age;
        protected String name;

        public BasePerson() {
        }

        public BasePerson(int age, String name) {
            this.age=age;
            this.name=name;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(age);
            Bits.writeString(name, out);
        }

        public void readFrom(DataInput in) throws IOException {
            age=in.readInt();
            name=Bits.readString(in);
        }

        public String toString() {
            return String.format("name=%s, age=%d", name, age);
        }
    }

    protected static class Person extends BasePerson implements SizeStreamable {

        public Person() {
        }

        public Person(int age, String name) {
            super(age, name);
        }

        public int serializedSize() {
            return Global.INT_SIZE + Bits.size(name);
        }
    }


}
