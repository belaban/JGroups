package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Headers;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Map;

/**
 * Tests the functionality of the Headers class
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class HeadersTest {
    private static final short UDP_ID=1, FRAG_ID=2, NAKACK_ID=3;

    private static final MyHeader h1=new MyHeader(), h2=new MyHeader(), h3=new MyHeader();



    public static void testConstructor() {
        Headers hdrs=new Headers(5);
        System.out.println("hdrs = " + hdrs);
        assert hdrs.capacity() == 5 : "capacity must be 5 but was " + hdrs.capacity();
        short[] ids=hdrs.getRawIDs();
        assert ids.length == hdrs.capacity();
        Header[] headers=hdrs.getRawHeaders();
        assert headers.length == hdrs.capacity();
        assert hdrs.size() == 0;
    }


    public static void testContructor2() {
        Headers old=createHeaders(3);

        Headers hdrs=old.copy();
        System.out.println("hdrs = " + hdrs);
        assert hdrs.capacity() == 3 : "capacity must be 3 but was " + hdrs.capacity();

        short[] ids=hdrs.getRawIDs();
        Header[] headers=hdrs.getRawHeaders();

        assert ids.length == hdrs.capacity();
        assert headers.length == hdrs.capacity();

        assert hdrs.size() == 3;

        // make sure 'hdrs' is not changed when 'old' is modified, as 'hdrs' is a copy
        old.putHeader((short)300, new MyHeader());
        assert hdrs.capacity() == 3 : "capacity must be 3 but was " + hdrs.capacity();

        assert ids.length == hdrs.capacity();
        assert headers.length == hdrs.capacity();

        assert hdrs.size() == 3;
    }


    public static void testGetRawData() {
        Headers hdrs=createHeaders(3);

        short[] ids=hdrs.getRawIDs();
        Header[] headers=hdrs.getRawHeaders();

        assert ids.length == 3;
        assert headers.length == 3;

        assert ids[0] == NAKACK_ID;
        assert headers[0] == h1;

        assert ids[1] == FRAG_ID;
        assert headers[1] == h2;

        assert ids[2] == UDP_ID;
        assert headers[2] == h3;

        assert ids.length == hdrs.capacity();
        assert headers.length == hdrs.capacity();

        assert hdrs.size() == 3;
    }


    public static void testGetHeaders() {
        Headers hdrs=createHeaders(3);
        Map<Short, Header> map=hdrs.getHeaders();
        System.out.println("map = " + map);
        assert map != null && map.size() == 3;
        assert map.get(NAKACK_ID) == h1;
        assert map.get(FRAG_ID) == h2;
        assert map.get(UDP_ID) == h3;
    }

    public static void testSize() {
        Headers hdrs=createHeaders(3);
        assert hdrs.size() == 3;
    }


    private static Headers createHeaders(int initial_capacity) {
        Headers hdrs=new Headers(initial_capacity);
        hdrs.putHeader(NAKACK_ID,  h1);
        hdrs.putHeader(FRAG_ID, h2);
        hdrs.putHeader(UDP_ID, h3);
        return hdrs;
    }


    public static void testPutHeader() {
        Headers hdrs=createHeaders(3);
        assert hdrs.getHeader(NAKACK_ID) == h1;
        hdrs.putHeader(NAKACK_ID, new MyHeader());
        assert hdrs.size() == 3;
        assert hdrs.getHeader(NAKACK_ID) != h1;
        assert hdrs.capacity() == 3;

        hdrs.putHeader((short)400, new MyHeader());
        assert hdrs.size() == 4;
        assert hdrs.capacity() > 3;
    }


    public static void testPutHeaderIfAbsent() {
        Headers hdrs=createHeaders(3);
        Header hdr=hdrs.putHeaderIfAbsent(FRAG_ID, new MyHeader());
        assert hdr == h2;
        assert hdr == hdrs.getHeader(FRAG_ID);
        assert hdrs.size() == 3;
        assert hdrs.capacity() == 3;

        hdr=hdrs.putHeaderIfAbsent((short)400, new MyHeader());
        System.out.println("hdrs = " + hdrs);
        assert hdr == null;
        assert hdrs.size() == 4;
        assert hdrs.capacity() == 6;

        hdrs.putHeader(FRAG_ID,null);
        assert hdrs.getHeader(FRAG_ID) == null;
        MyHeader myhdr=new MyHeader();
        hdr=hdrs.putHeaderIfAbsent(FRAG_ID, myhdr);
        assert hdr == null;
        assert hdrs.getHeader(FRAG_ID) == myhdr;
    }

    public static void testGetHeader() {
        Headers hdrs=createHeaders(3);
        assert null == hdrs.getHeader((short)400);
        assert hdrs.getHeader(UDP_ID) == h3;
    }


    public static void testResize() {
        Headers hdrs=createHeaders(3);
        int capacity=hdrs.capacity();
        System.out.println("hdrs = " + hdrs + ", capacity=" + capacity);

        hdrs.putHeader((short)400, new MyHeader());
        System.out.println("hdrs = " + hdrs + ", capacity=" + hdrs.capacity());
        assert hdrs.capacity() > capacity;

        capacity=hdrs.capacity();
        for(int i=10; i <= 13; i++)
            hdrs.putHeader((short)i, new MyHeader());
        System.out.println("hdrs = " + hdrs + ", capacity=" + hdrs.capacity());
        assert hdrs.capacity() > capacity;
    }


    public static class MyHeader extends Header {

        public MyHeader() {
        }

        public String toString() {
            return "MyHeader";
        }

        public void writeTo(DataOutput out) throws Exception {
        }

        public void readFrom(DataInput in) throws Exception {
        }

        public int size() {
            return 0;
        }
    }
}
