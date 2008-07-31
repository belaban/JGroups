package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Headers;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Tests the functionality of the Headers class
 * @author Bela Ban
 * @version $Id: HeadersTest.java,v 1.4 2008/07/31 12:53:51 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class HeadersTest {
    private static final String UDP="UDP", FRAG="FRAG", NAKACK="NAKACK";
    private static final MyHeader h1=new MyHeader(), h2=new MyHeader(), h3=new MyHeader();



    public static void testConstructor() {
        Headers hdrs=new Headers(5);
        System.out.println("hdrs = " + hdrs);
        assert hdrs.capacity() == 5 : "capacity must be 5 but was " + hdrs.capacity();
        Object[] data=hdrs.getRawData();
        assert data.length == hdrs.capacity() * 2;
        assert hdrs.size() == 0;
    }


    public static void testContructor2() {
        Headers old=createHeaders(3);

        Headers hdrs=new Headers(old);
        System.out.println("hdrs = " + hdrs);
        assert hdrs.capacity() == 3 : "capacity must be 3 but was " + hdrs.capacity();
        Object[] data=hdrs.getRawData();
        assert data.length == hdrs.capacity() * 2;
        assert hdrs.size() == 3;

        // make sure 'hdrs' is not changed when 'old' is modified, as 'hdrs' is a copy
        old.putHeader("BLA", new MyHeader());
        assert hdrs.capacity() == 3 : "capacity must be 3 but was " + hdrs.capacity();
        data=hdrs.getRawData();
        assert data.length == hdrs.capacity() * 2;
        assert hdrs.size() == 3;
    }


    public static void testGetRawData() {
        Headers hdrs=createHeaders(3);
        Object[] data=hdrs.getRawData();
        assert data.length == 6;
        assert data[0].equals(NAKACK);
        assert data[1].equals(h1);
        assert data[2].equals(FRAG);
        assert data[3].equals(h2);
        assert data[4].equals(UDP);
        assert data[5].equals(h3);
        assert data.length == hdrs.capacity() * 2;
        assert hdrs.size() == 3;
    }


    public static void testGetHeaders() {
        Headers hdrs=createHeaders(3);
        Map<String, Header> map=hdrs.getHeaders();
        System.out.println("map = " + map);
        assert map != null && map.size() == 3;
        assert map.get(NAKACK) == h1;
        assert map.get(FRAG) == h2;
        assert map.get(UDP) == h3;
    }

    public static void testSize() {
        Headers hdrs=createHeaders(3);
        assert hdrs.size() == 3;
    }


    private static Headers createHeaders(int initial_capacity) {
        Headers hdrs=new Headers(initial_capacity);
        hdrs.putHeader(NAKACK,  h1);
        hdrs.putHeader(FRAG, h2);
        hdrs.putHeader(UDP, h3);
        return hdrs;
    }


    public static void testPutHeader() {
        Headers hdrs=createHeaders(3);
        assert hdrs.getHeader(NAKACK) == h1;
        hdrs.putHeader(NAKACK, new MyHeader());
        assert hdrs.size() == 3;
        assert hdrs.getHeader(NAKACK) != h1;
        assert hdrs.capacity() == 3;

        hdrs.putHeader("NEW", new MyHeader());
        assert hdrs.size() == 4;
        assert hdrs.capacity() > 3;
    }


    public static void testPutHeaderIfAbsent() {
        Headers hdrs=createHeaders(3);
        Header hdr=hdrs.putHeaderIfAbsent(FRAG, new MyHeader());
        assert hdr == h2;
        assert hdr == hdrs.getHeader(FRAG);
        assert hdrs.size() == 3;
        assert hdrs.capacity() == 3;

        hdr=hdrs.putHeaderIfAbsent("NEW", new MyHeader());
        System.out.println("hdrs = " + hdrs);
        assert hdr == null;
        assert hdrs.size() == 4;
        assert hdrs.capacity() == 6;
    }

    public static void testGetHeader() {
        Headers hdrs=createHeaders(3);
        assert null == hdrs.getHeader("NOTAVAILABLE");
        assert hdrs.getHeader(UDP) == h3;
    }


    public static void testResize() {
        Headers hdrs=createHeaders(3);
        int capacity=hdrs.capacity();
        System.out.println("hdrs = " + hdrs + ", capacity=" + capacity);

        hdrs.putHeader("NEW", new MyHeader());
        System.out.println("hdrs = " + hdrs + ", capacity=" + hdrs.capacity());
        assert hdrs.capacity() > capacity;

        capacity=hdrs.capacity();
        for(int i=0; i < 3; i++)
            hdrs.putHeader("#" + i, new MyHeader());
        System.out.println("hdrs = " + hdrs + ", capacity=" + hdrs.capacity());
        assert hdrs.capacity() > capacity;
    }


    public static class MyHeader extends Header {
        private static final long serialVersionUID=-7164974484154022976L;

        public MyHeader() {
        }

        public String toString() {
            return "MyHeader";
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }
}
