package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.AbstractHeader;
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
    private static final short NAKACK_ID=1, FRAG_ID=2, UDP_ID=3;

    private static final MyHeader h1=new MyHeader(NAKACK_ID), h2=new MyHeader(FRAG_ID), h3=new MyHeader(UDP_ID);



    public void testGetHeader() {
        AbstractHeader[] hdrs=createHeaders(3);
        assert null == Headers.getHeader(null, (short)400);
        assert null == Headers.getHeader(hdrs, (short)400);
        assert Headers.getHeader(hdrs, UDP_ID) == h3;
    }


    public void testGetHeaders() {
        AbstractHeader[] hdrs=createHeaders(3);
        System.out.printf("hdrs are: %s\n", Headers.printObjectHeaders(hdrs));
        Map<Short, AbstractHeader> map=Headers.getHeaders(hdrs);
        System.out.println("map = " + map);
        assert map != null && map.size() == 3;
        assert map.get(NAKACK_ID) == h1;
        assert map.get(FRAG_ID) == h2;
        assert map.get(UDP_ID) == h3;
    }


    public void testPutHeader() {
        AbstractHeader[] hdrs=createHeaders(3);
        assert Headers.getHeader(hdrs, NAKACK_ID) == h1;
        AbstractHeader[] retval=Headers.putHeader(hdrs, NAKACK_ID, new MyHeader(NAKACK_ID), true);
        assert retval == null;
        assert Headers.size(hdrs) == 3;
        assert Headers.getHeader(hdrs, NAKACK_ID) != h1;

        retval=Headers.putHeader(hdrs, (short)400, new MyHeader((short)400), true);
        assert retval != null;
        hdrs=retval;
        assert Headers.size(hdrs) == 4;
        assert hdrs.length > 3;
    }


    public void testPutHeaderIfAbsent() {
        AbstractHeader[] hdrs=createHeaders(3);
        AbstractHeader[] retval=Headers.putHeader(hdrs, FRAG_ID, new MyHeader(FRAG_ID), false);
        assert retval == null;

        assert Headers.getHeader(hdrs, FRAG_ID) == h2;
        assert Headers.size(hdrs) == 3;
        assert hdrs.length == 3;

        retval=Headers.putHeader(hdrs, (short)400, new MyHeader((short)400), false);
        assert retval != null;
        hdrs=retval;

        String tmp=Headers.printHeaders(hdrs);
        System.out.printf("headers are %s\n", tmp);

        assert Headers.size(hdrs) == 4;
        assert hdrs.length == 6;

        Headers.putHeader(hdrs, FRAG_ID,null, true);
        assert Headers.getHeader(hdrs, FRAG_ID) == null;

        MyHeader myhdr=new MyHeader(FRAG_ID);
        retval=Headers.putHeader(hdrs, FRAG_ID, myhdr, false);
        assert retval == null;
        assert Headers.getHeader(hdrs, FRAG_ID) == myhdr;
    }


    public void testResize() {
        AbstractHeader[] hdrs=createHeaders(3);
        int capacity=hdrs.length;
        System.out.println("hdrs = " + Headers.printHeaders(hdrs) + ", capacity=" + capacity);

        AbstractHeader[] retval=Headers.putHeader(hdrs, (short)400, new MyHeader((short)400), true);
        assert retval != null;
        hdrs=retval;
        System.out.println("hdrs = " + Headers.printHeaders(hdrs) + ", capacity=" + hdrs.length);
        assert hdrs.length > capacity;

        capacity=hdrs.length;
        for(int i=10; i <= 13; i++) {
            retval=Headers.putHeader(hdrs, (short)i, new MyHeader((short)i), true);
            if(retval != null)
                hdrs=retval;
        }
        System.out.println("hdrs = " + Headers.printHeaders(hdrs) + ", capacity=" + hdrs.length);
        assert hdrs.length > capacity;
    }


    public void testCopy() {
        AbstractHeader[] hdrs=createHeaders(3);
        AbstractHeader[] retval=Headers.putHeader(hdrs, (short)400, new MyHeader((short)400), true);
        assert retval != null;
        hdrs=retval;
        AbstractHeader[] copy=Headers.copy(hdrs);
        assert copy.length == hdrs.length;
        assert Headers.size(copy) == Headers.size(hdrs);
    }


    public void testSize() {
        AbstractHeader[] hdrs=createHeaders(3);
        assert Headers.size(hdrs) == 3;
    }


    private static AbstractHeader[] createHeaders(int initial_capacity) {
        AbstractHeader[] hdrs=new AbstractHeader[initial_capacity];
        hdrs[0]=h1;
        hdrs[1]=h2;
        hdrs[2]=h3;
        return hdrs;
    }



    public static class MyHeader extends AbstractHeader {

        public MyHeader(short prot_id) {
            this.prot_id=prot_id;
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
