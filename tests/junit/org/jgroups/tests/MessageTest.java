// $Id: MessageTest.java,v 1.1 2004/02/25 20:40:15 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;



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
        assertNotNull(m1.getBuffer());
        assertEquals(m1.getBuffer().length, m1.getBufferSize());
        byte[] new_buf={'m', 'i', 'c', 'h', 'e', 'l', 'l', 'e'};
        m1.setBuffer(new_buf);
        assertNotNull(m1.getBuffer());
        assertEquals(new_buf.length, m1.getBufferSize());
        assertEquals(m1.getBuffer().length, m1.getBufferSize());
    }

    public void testBufferOffset() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);

        byte[] b1, b2;

        b1=new byte[m1.getBufferSize()];
        System.arraycopy(m1.getBuffer(), m1.getOffset(), b1, 0, m1.getBufferSize());

        b2=new byte[m2.getBufferSize()];
        System.arraycopy(m2.getBuffer(), m2.getOffset(), b2, 0, m2.getBufferSize());

        assertEquals(4, b1.length);
        assertEquals(3, b2.length);
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
        assertEquals(4, m3.getBufferSize());
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
        assertEquals(3, m4.getBufferSize());
        assertEquals(3, m4.getBuffer().length);
        assertEquals(0, m4.getOffset());
    }


    public void testSetObject() {
        String s1="Bela Ban";
        m1=new Message(null, null, s1);
        assertEquals(0, m1.getOffset());
        assertEquals(m1.getBuffer().length, m1.getBufferSize());
        String s2=(String)m1.getObject();
        assertEquals(s2, s1);
    }

    public void testReset() {
        m1=new Message(null, null, "Bela Ban");
        m1.reset();
        assertEquals(0, m1.getOffset());
        assertEquals(0, m1.getBufferSize());
        assertNull(m1.getBuffer());
    }

    public void testCopy() {
        m1=new Message(null, null, "Bela Ban");
        m2=m1.copy();
        assertEquals(m1.getOffset(), m2.getOffset());
        assertEquals(m1.getBufferSize(), m2.getBufferSize());
    }


    public void testCopyWithOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        m1=new Message(null, null, buf, 0, 4);
        m2=new Message(null, null, buf, 4, 3);

        Message m3, m4;
        m3=m1.copy();
        m4=m2.copy();

        assertEquals(m1.getOffset(), m3.getOffset());
        assertEquals(m1.getBufferSize(), m3.getBufferSize());

        assertEquals(m2.getOffset(), m4.getOffset());
        assertEquals(m2.getBufferSize(), m4.getBufferSize());
    }

    public static Test suite() {
        TestSuite s=new TestSuite(MessageTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
