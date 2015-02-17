package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AsciiString;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link AsciiString}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class AsciiStringTest {

    public void testCreation() {
        String orig="hello";
        AsciiString str=new AsciiString(orig);
        assert str.length() == orig.length();
        assert str.toString().equals(orig);

        AsciiString str2=new AsciiString(str);
        assert str2.equals(str);
        assert str2.length() == str.length();

        str2=new AsciiString(new byte[]{'h', 'e', 'l', 'l', 'o'});
        assert str2.equals(str);
        assert str2.length() == str.length();
    }

    public void testCompareTo() {
        AsciiString str=new AsciiString("hello"), str2=new AsciiString("hello world");
        int comp=str.compareTo(str2);
        assert comp < 0;
    }

    public void testHashcode() {
        AsciiString str=new AsciiString("hello"), str2=new AsciiString("hello");
        assert str.hashCode() == str2.hashCode();

        str2=new AsciiString("hello world");
        assert str.hashCode() != str2.hashCode();

        Map<AsciiString,Integer> map=new HashMap<>(5);
        map.put(new AsciiString("a"), 1);
        assert map.get(new AsciiString("a")) == 1;

        map.put(new AsciiString("b"), 2);
        assert map.get(new AsciiString("b")) == 2;

        map.put(new AsciiString("a"), 2);
        assert map.get(new AsciiString("a")) == 2;
    }
}
