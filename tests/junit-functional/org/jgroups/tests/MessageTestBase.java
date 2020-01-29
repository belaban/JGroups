package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.testng.Assert;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base class for common methods
 * @author Bela Ban
 * @since  5.0.0
 */
public class MessageTestBase {
    protected static final short UDP_ID=101;
    protected static final short PING_ID=102;
    protected static final short NAKACK_ID=103;

    protected static void addHeaders(Message msg) {
        TpHeader tp_hdr=new TpHeader("DemoChannel2");
        msg.putHeader(UDP_ID, tp_hdr);
        PingHeader ping_hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName("demo-cluster");
        msg.putHeader(PING_ID, ping_hdr);
        NakAckHeader2 nak_hdr=NakAckHeader2.createXmitRequestHeader(Util.createRandomAddress("S"));
        msg.putHeader(NAKACK_ID, nak_hdr);
    }

    protected static Message makeReply(Message msg) {
        Message reply=msg.create().get().setDest(msg.getSrc());
        if(msg.getDest() != null)
            reply.setSrc(msg.getDest());
        return reply;
    }

    protected static void _testSize(Message msg) throws Exception {
        int size=msg.size();
        byte[] serialized_form=Util.streamableToByteBuffer(msg);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(size, serialized_form.length);
    }

    protected static byte[] marshal(Message msg) throws Exception {
        return Util.streamableToByteBuffer(msg);
    }

    protected static Message unmarshal(Class<? extends Message> cl, byte[] buf) throws Exception {
        return Util.streamableFromByteBuffer(cl, buf);
    }

    protected static Message unmarshal(Class<? extends Message> cl, ByteBuffer buf) throws Exception {
        return Util.streamableFromByteBuffer(cl, buf);
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
