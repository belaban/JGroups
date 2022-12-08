package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ObjectWrapperSerializable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests {@link ObjectWrapperSerializable}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class ObjectWrapperTest {
    protected static final String HELLO="hello";
    protected static final byte[] HELLO_SER;

    protected static final String WORLD="world";
    protected static final byte[] WORLD_SER;

    static {
        try {
            HELLO_SER=Util.objectToByteBuffer(HELLO);
            WORLD_SER=Util.objectToByteBuffer(WORLD);
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testConstructor() {
        ObjectWrapperSerializable o=new ObjectWrapperSerializable(HELLO);
        assert o.getObject().equals(HELLO);
    }

    public void testConstructor2() {
        ObjectWrapperSerializable o=new ObjectWrapperSerializable(HELLO_SER);
        byte[] arr=o.getObject();
        assert Arrays.equals(arr, HELLO_SER);
    }


    public void testSetObject() {
        ObjectWrapperSerializable o=new ObjectWrapperSerializable(HELLO);
        o.getLength();
        o.setObject(WORLD);
        String s=o.getObject();
        assert s.equals(WORLD);
        byte[] ser=o.getSerialized().getArray();
        assert Arrays.equals(ser, WORLD_SER);
    }

    public void testSerialization() throws Exception {
        ObjectWrapperSerializable o=new ObjectWrapperSerializable(HELLO);
        byte[] buf=Util.streamableToByteBuffer(o);

        ObjectWrapperSerializable o2=Util.streamableFromByteBuffer(ObjectWrapperSerializable::new, buf);
        assert o.getObject().equals(o2.getObject());
    }
}
