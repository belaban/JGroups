package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Headers;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Tests the functionality of the Headers class
 * @author Bela Ban
 * @version $Id: HeadersTest.java,v 1.2 2008/07/30 09:50:22 belaban Exp $
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
        assert data.length == hdrs.capacity() / 2;
        assert hdrs.size() == 3;
    }



    private static Headers createHeaders(int initial_capacity) {
        Headers hdrs=new Headers(initial_capacity);
        hdrs.putHeader(NAKACK,  h1);
        hdrs.putHeader(FRAG, h2);
        hdrs.putHeader(UDP, h3);
        return hdrs;
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
