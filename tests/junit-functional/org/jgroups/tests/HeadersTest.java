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
 * @version $Id: HeadersTest.java,v 1.1 2008/07/30 09:07:35 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class HeadersTest {
    private static final String UDP="UDP", FRAG="FRAG", NAKACK="NAKACK";
    private final MyHeader h1=new MyHeader(), h2=new MyHeader(), h3=new MyHeader();



    public static void testConstructor() {
        Headers hdrs=new Headers(5);
        System.out.println("hdrs = " + hdrs);
        assert hdrs.capacity() == 5 : "capacity must be 5 but was " + hdrs.capacity();
        Object[] data=hdrs.getRawData();
        assert data.length == hdrs.capacity() * 2;
        assert hdrs.size() == 0;
    }



    public static class MyHeader extends Header {
        private static final long serialVersionUID=-7164974484154022976L;

        public MyHeader() {
        }

        public void writeExternal(ObjectOutput out) throws IOException {
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }
}
