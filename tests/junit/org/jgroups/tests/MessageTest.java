// $Id: MessageTest.java,v 1.9 2004/03/01 03:03:47 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.util.Range;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;



public class MessageTest extends TestCase {
    Message m1, m2;


    public MessageTest(String name) {
        super(name);
    }


    public void setUp() {

    }

    public void tearDown() {
        
    }

 
    public void testBufferSize() throws Exception {
        m1=new Message(null, null, "bela");
        assertNotNull(m1.getRawBuffer());
        assertNotNull(m1.getBuffer());
        assertEquals(m1.getBuffer().length, m1.getLength());
        byte[] new_buf={'m', 'i', 'c', 'h', 'e', 'l', 'l', 'e'};
        m1.setBuffer(new_buf);
        assertNotNull(m1.getRawBuffer());
        assertNotNull(m1.getBuffer());
        assertEquals(new_buf.length, m1.getLength());
        assertEquals(m1.getBuffer().length, m1.getLength());
    }

    public void testBufferOffset() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);

        byte[] b1, b2;

        b1=new byte[m1.getLength()];
        System.arraycopy(m1.getRawBuffer(), m1.getOffset(), b1, 0, m1.getLength());

        b2=new byte[m2.getLength()];
        System.arraycopy(m2.getRawBuffer(), m2.getOffset(), b2, 0, m2.getLength());

        assertEquals(4, b1.length);
        assertEquals(3, b2.length);
    }


    public void testInvalidOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};

        try {
            m1=new Message(null, null, buf, -1, 4);
            fail("we should not get here (offset is -1)");
        }
        catch(ArrayIndexOutOfBoundsException ex) {
            assertTrue("correct: offset is invalid (caught correctly)", true);
        }
    }

    public void testInvalidLength() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};

        try {
            m1=new Message(null, null, buf, 3, 6);
            fail("we should not get here (length is 9)");
        }
        catch(ArrayIndexOutOfBoundsException ex) {
            assertTrue("correct: length is invalid (caught correctly)", true);
        }
    }

    public void testGetRawBuffer() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);

        assertEquals(buf.length, m1.getRawBuffer().length);
        assertEquals(4, m1.getBuffer().length);
        assertEquals(4, m1.getLength());

        assertEquals(buf.length, m2.getRawBuffer().length);
        assertEquals(3, m2.getBuffer().length);
        assertEquals(3, m2.getLength());
    }

    public void testSerialization() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        byte[] tmp;
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);


        ByteArrayOutputStream output=new ByteArrayOutputStream();
        ObjectOutputStream out=new ObjectOutputStream(output);
        out.writeObject(m1);
        output.close();
        tmp=output.toByteArray();

        ByteArrayInputStream input=new ByteArrayInputStream(tmp);
        ObjectInputStream in=new ObjectInputStream(input);
        Message m3, m4;

        m3=(Message)in.readObject();
        assertEquals(4, m3.getLength());
        assertEquals(4, m3.getRawBuffer().length);
        assertEquals(4, m3.getBuffer().length);
        assertEquals(0, m3.getOffset());

        output=new ByteArrayOutputStream();
        out=new ObjectOutputStream(output);
        out.writeObject(m2);
        output.close();
        tmp=output.toByteArray();

        input=new ByteArrayInputStream(tmp);
        in=new ObjectInputStream(input);
        m4=(Message)in.readObject();
        assertEquals(3, m4.getLength());
        assertEquals(3, m4.getBuffer().length);
        assertEquals(3, m4.getRawBuffer().length);
        assertEquals(0, m4.getOffset());
    }


    public void testSetObject() {
        String s1="Bela Ban";
        m1=new Message(null, null, s1);
        assertEquals(0, m1.getOffset());
        assertEquals(m1.getBuffer().length, m1.getLength());
        String s2=(String)m1.getObject();
        assertEquals(s2, s1);
    }

    public void testReset() {
        m1=new Message(null, null, "Bela Ban");
        m1.reset();
        assertEquals(0, m1.getOffset());
        assertEquals(0, m1.getLength());
        assertNull(m1.getBuffer());
        assertNull(m1.getRawBuffer());
    }

    public void testCopy() {
        m1=new Message(null, null, "Bela Ban");
        m2=m1.copy();
        assertEquals(m1.getOffset(), m2.getOffset());
        assertEquals(m1.getLength(), m2.getLength());
    }


    public void testCopyWithOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);

        Message m3, m4;
        m3=m1.copy();
        m4=m2.copy();

        assertEquals(0, m3.getOffset());
        assertEquals(4, m3.getLength());
        assertEquals(4, m3.getBuffer().length);

        assertEquals(4, m4.getOffset());
        assertEquals(3, m4.getLength());
        assertEquals(3, m4.getBuffer().length);
    }

    public void testComputeFragOffsets() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 4);
        System.out.println("list is " + retval);
        assertEquals(3, retval.size());
        r=(Range)retval.get(0);
        assertEquals(0, r.low);
        assertEquals(4, r.high);

        r=(Range)retval.get(1);
        assertEquals(4, r.low);
        assertEquals(4, r.high);

        r=(Range)retval.get(2);
        assertEquals(8, r.low);
        assertEquals(2, r.high);
    }


    public void testComputeFragOffsetsWithOffsets() {
        Range r;
        // byte[] buf={'p', 'a', 'd', 0,1,2,3,4,5,6,7,8,9, 'p', 'a', 'd', 'd', 'i', 'e'};
        java.util.List retval=Util.computeFragOffsets(3, 10, 4);
        System.out.println("list is " + retval);
        assertEquals(3, retval.size());
        r=(Range)retval.get(0);
        assertEquals(3, r.low);
        assertEquals(4, r.high);

        r=(Range)retval.get(1);
        assertEquals(7, r.low);
        assertEquals(4, r.high);

        r=(Range)retval.get(2);
        assertEquals(11, r.low);
        assertEquals(2, r.high);
    }

    public void testComputeFragOffsets2() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 10);
        System.out.println("list is " + retval);
        assertEquals(1, retval.size());
        r=(Range)retval.get(0);
        assertEquals(0, r.low);
        assertEquals(10, r.high);
    }

    public void testComputeFragOffsets3() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 100);
        System.out.println("list is " + retval);
        assertEquals(1, retval.size());
        r=(Range)retval.get(0);
        assertEquals(0, r.low);
        assertEquals(10, r.high);
    }

    public void testComputeFragOffsets4() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 5);
        System.out.println("list is " + retval);
        assertEquals(2, retval.size());
        r=(Range)retval.get(0);
        assertEquals(0, r.low);
        assertEquals(5, r.high);

        r=(Range)retval.get(1);
        assertEquals(5, r.low);
        assertEquals(5, r.high);
    }

    public static Test suite() {
        TestSuite s=new TestSuite(MessageTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
