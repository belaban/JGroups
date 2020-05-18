package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ObjectWrapper;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests {@link org.jgroups.util.ObjectWrapper}
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
        ObjectWrapper o=new ObjectWrapper(HELLO);
        assert o.getObject().equals(HELLO);
    }

    public void testConstructor2() {
        ObjectWrapper o=new ObjectWrapper(HELLO_SER);
        byte[] arr=o.getObject();
        assert Arrays.equals(arr, HELLO_SER);
    }


    public void testSetObject() {
        ObjectWrapper o=new ObjectWrapper(HELLO);
        o.getLength();
        o.setObject(WORLD);
        String s=o.getObject();
        assert s.equals(WORLD);
        byte[] ser=o.getSerialized().getArray();
        assert Arrays.equals(ser, WORLD_SER);
    }

    public void testSerialization() throws Exception {
        ObjectWrapper o=new ObjectWrapper(HELLO);
        byte[] buf=Util.streamableToByteBuffer(o);

        ObjectWrapper o2=Util.streamableFromByteBuffer(ObjectWrapper::new, buf);
        assert o.getObject().equals(o2.getObject());
    }
}
