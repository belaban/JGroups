// $Id: UtilTest.java,v 1.1 2004/10/07 14:45:18 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;

import java.io.*;




public class UtilTest extends TestCase {


    public UtilTest(String name) {
        super(name);
    }


    public void setUp() {

    }

    public void tearDown() {
    }


    public void testWriteStreamable() throws IOException, IllegalAccessException, InstantiationException {
        Message m=new Message(null, null, "Hello");
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        m.writeTo(dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Message m2=new Message();
        m2.readFrom(dis);
        assertNotNull(m2.getBuffer());
        assertEquals(m.getLength(), m2.getLength());
    }



    public static Test suite() {
        TestSuite s=new TestSuite(UtilTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


