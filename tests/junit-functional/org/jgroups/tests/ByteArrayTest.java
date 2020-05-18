package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ByteArray;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class ByteArrayTest {
    protected static final byte[] HELLO="hello world".getBytes();

    public void testGetBytes() {
        ByteArray ba=new ByteArray(HELLO);
        byte[] bytes=ba.getBytes();
        assert Arrays.equals(bytes, HELLO);
        assert HELLO.hashCode() == bytes.hashCode(); // same memory location

        ba=new ByteArray(HELLO, 0, 5);
        bytes=ba.getBytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("hello");

        ba=new ByteArray(HELLO, 6, 5);
        bytes=ba.getBytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("world");

    }
}
