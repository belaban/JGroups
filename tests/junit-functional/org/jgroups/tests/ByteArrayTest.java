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
        byte[] bytes=ba.bytes();
        assert Arrays.equals(bytes, HELLO);
        assert HELLO.hashCode() == bytes.hashCode(); // same memory location

        ba=new ByteArray(HELLO, 0, 5);
        bytes=ba.bytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("hello");

        ba=new ByteArray(HELLO, 6, 5);
        bytes=ba.bytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("world");
    }

    public void testCopy() {
        ByteArray ba=new ByteArray(HELLO), ba2=ba.copy();
        byte[] bytes=ba2.bytes();
        assert Arrays.equals(bytes, HELLO);

        ba=new ByteArray(HELLO, 0, 5);
        ba2=ba.copy();
        bytes=ba2.bytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("hello");
        assert ba.length() == ba2.length();
        assert ba.array().length > ba2.array().length;

        ba=new ByteArray(HELLO, 6, 5);
        ba2=ba.copy();
        bytes=ba2.bytes();
        assert HELLO.hashCode() != bytes.hashCode();
        assert new String(bytes).equals("world");
        assert ba.length() == ba2.length();
        assert ba.array().length > ba2.array().length;
    }
}
