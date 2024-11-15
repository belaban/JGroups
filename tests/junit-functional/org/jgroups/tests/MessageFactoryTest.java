package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.ByteArray;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageFactoryTest {

    public void testRegistration() {
        for(int i=0; i < 32; i++) {
            try {
                MessageFactory.register((short)i, MyMessageFactory::new);
            }
            catch(IllegalArgumentException ex) {
                System.out.printf("received exception (as expected): %s\n", ex);
            }
        }
        MessageFactory.register((short)32, MyMessageFactory::new);

        try {
            MessageFactory.register((short)32, MyMessageFactory::new);
        }
        catch(IllegalArgumentException ex) {
            System.out.printf("received exception (as expected): %s\n", ex);
        }
    }


    protected static class MyMessageFactory extends BaseMessage {

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
