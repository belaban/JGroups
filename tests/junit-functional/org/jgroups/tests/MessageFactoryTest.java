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
    protected final MessageFactory mf=new DefaultMessageFactory();

    public void testRegistration() {
        for(int i=0; i < 32; i++) {
            try {
                mf.register((short)i, MyMessageFactory::new);
            }
            catch(IllegalArgumentException ex) {
                System.out.printf("received exception (as expected): %s\n", ex);
            }
        }
        mf.register((short)32, MyMessageFactory::new);

        try {
            mf.register((short)32, MyMessageFactory::new);
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

        public <T extends Message> T setArray(byte[] b, int offset, int length) {
            return null;
        }

        public <T extends Message> T setArray(ByteArray buf) {
            return null;
        }

        public <T> T getObject() {
            return null;
        }

        public <T extends Message> T setObject(Object obj) {
            return null;
        }

        protected void writePayload(DataOutput out) throws IOException {
        }

        protected void readPayload(DataInput in) throws IOException, ClassNotFoundException {
        }
    }
}
