package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests {@link org.jgroups.util.MillisAddress}
 * @author Bela Ban
 * @since  5.5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class MillisAddressTest {

    public void testCreation() {
        MillisAddress addr=new MillisAddress(1);
        assert addr.millis() == 1;
    }

    public void testCompareTo() {
        Address a=new MillisAddress(1), b=new MillisAddress(2), c=new MillisAddress(1);
        //noinspection EqualsWithItself
        assert a.compareTo(a) == 0;
        assert a.compareTo(b) < 0;
        assert b.compareTo(a) > 0;
        assert a.compareTo(c) == 0;
        assert c.compareTo(a) == 0;
    }

    public void testEquals() {
        Address a=new MillisAddress(1), b=new MillisAddress(2), c=new MillisAddress(1);
        //noinspection EqualsWithItself
        assert a.equals(a);
        assert !a.equals(b);
        assert !b.equals(a);
        assert a.equals(c);
        assert c.equals(a);
    }

    public void testSize() throws IOException, ClassNotFoundException {
        MillisAddress addr=new MillisAddress(1);
        int size=Util.size(addr);
        ByteArray buf=writeAddress(addr);
        assert buf.getLength() == size;
        Address a2=readAddress(buf);
        assert addr.equals(a2);
        assert a2.equals(addr);
    }

    public void testHashcode() {
        Address a=new MillisAddress(1), b=new MillisAddress(1), c=new MillisAddress(1);
        ConcurrentHashMap<Address,Integer> s=new ConcurrentHashMap<>(3);
        s.put(a,1);
        s.put(b,2);
        s.put(c,3);
        assert s.size() == 1;
    }

    protected static ByteArray writeAddress(Address addr) throws IOException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(addr.serializedSize());
        Util.writeAddress(addr, out);
        return out.getBuffer();
    }

    protected static Address readAddress(ByteArray buf) throws IOException, ClassNotFoundException {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);
        return Util.readAddress(in);
    }


}
