 package org.jgroups.tests;

 import org.jgroups.*;
 import org.jgroups.util.ByteArray;
 import org.jgroups.util.MyReceiver;
 import org.jgroups.util.Util;
 import org.testng.annotations.Test;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.io.IOException;
 import java.util.List;
 import java.util.function.Supplier;
 import java.util.stream.Stream;

 /**
 * @author Bela Ban
 * @since  5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageFactoryTest {
    protected static final String CLUSTER=MessageFactoryTest.class.getSimpleName();

    public void testCustomMessages() throws Exception {
        try(JChannel a=new JChannel(Util.getTestStack()).name("A");
            JChannel b=new JChannel(Util.getTestStack()).name("B")) {
            MessageFactory mf=MessageFactory.createDefaultMessageFactory();
            mf.registerDefaultMessage((short)10, () -> new MyLongMessage("mf-10"));
            mf.registerDefaultMessage((short)11, () -> new MyLongMessage("mf-11"));
            mf.registerDefaultMessage((short)12, () -> new MyLongMessage("mf-12"));
            Stream.of(a, b).forEach(ch -> ch.stack().getTransport().setMessageFactory(mf));

            a.connect(CLUSTER);
            b.connect(CLUSTER);
            Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);

            MyReceiver<Message> r=new MyReceiver<Message>().rawMsgs(true);
            b.setReceiver(r);
            for(short i=10; i <= 12; i++) {
                Message msg=new MyLongMessage(b.address(), i, "m-" + i);
                a.send(msg);
            }
            List<Message> list=r.list();
            Util.waitUntil(2000, 100, () -> list.size() == 3);
            System.out.println("list = " + list);
        }
    }


    // todo: test large custom message with fragmentation (FRAG4)

    public void testRegistration() {
        MessageFactory mf=MessageFactory.get();
        for(int i=0; i < 32; i++) {
            try {
                mf.register((short)i, MyNewMessage::new);
            }
            catch(IllegalArgumentException ex) {
                System.out.printf("received exception (as expected): %s\n", ex);
            }
        }
        mf.register((short)32, MyNewMessage::new);

        try {
            mf.register((short)32, MyNewMessage::new);
        }
        catch(IllegalArgumentException ex) {
            System.out.printf("received exception (as expected): %s\n", ex);
        }
    }

    protected static class MyLongMessage extends LongMessage {
        protected String marshaller; // mimics the marshaller used

        public MyLongMessage() {
        }

        public MyLongMessage(String marshaller) {
            this.marshaller=marshaller;
        }

        public MyLongMessage(Address dest, long v, String marshaller) {
            super(dest, v);
            this.marshaller=marshaller;
        }

        @Override
        public short getType() {
            return (short)value;
        }

        @Override
        public String toString() {
            return super.toString() + " (marshaller: " + marshaller + ")";
        }
    }


    protected static class MyNewMessage extends BaseMessage {

        public short getType() {
            return 0;
        }

        public Supplier<Message> create() {
            return null;
        }

        public boolean hasPayload() {
            return false;
        }

        public boolean hasArray() {
            return false;
        }

        public byte[] getArray() {
            return new byte[0];
        }

        public int getOffset() {
            return 0;
        }

        public int getLength() {
            return 0;
        }

        public Message setArray(byte[] b, int offset, int length) {
            return null;
        }

        public Message setArray(ByteArray buf) {
            return null;
        }

        public <T> T getObject() {
            return null;
        }

        public Message setObject(Object obj) {
            return null;
        }

        public void writePayload(DataOutput out) throws IOException {
        }

        public void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        }

        protected int payloadSize() {
            return 0;
        }
    }
}
