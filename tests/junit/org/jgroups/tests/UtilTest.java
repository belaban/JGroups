// $Id: UtilTest.java,v 1.2 2004/10/07 15:05:13 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.ChannelException;
import org.jgroups.stack.IpAddress;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.io.*;




public class UtilTest extends TestCase {

    static {
        try {
            ClassConfigurator.getInstance(true);
        }
        catch(ChannelException e) {
            e.printStackTrace();
        }
    }

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
        Util.writeStreamable(m, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Message m2=(Message)Util.readStreamable(dis);
        assertNotNull(m2.getBuffer());
        assertEquals(m.getLength(), m2.getLength());
    }


    public void testWriteString() throws IOException {
        String s1="Bela Ban", s2="Michelle Ban";
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeString(s1, dos);
        Util.writeString(s2, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        String s3=Util.readString(dis);
        String s4=Util.readString(dis);
        assertEquals(s1, s3);
        assertEquals(s2, s4);
    }

    public void writeAddress() throws IOException, IllegalAccessException, InstantiationException {
        IpAddress a1=new IpAddress("localhost", 1234);
        IpAddress a2=new IpAddress("127.0.0.1", 4444);
        IpAddress a3=new IpAddress("thishostdoesnexist", 6666);
        IpAddress a4=new IpAddress("www.google.com", 7777);

        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeAddress(a1, dos);
        Util.writeAddress(a2, dos);
        Util.writeAddress(a3, dos);
        Util.writeAddress(a4, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);

        assertEquals(a1, Util.readAddress(dis));
        assertEquals(a2, Util.readAddress(dis));
        assertEquals(a3, Util.readAddress(dis));
        assertEquals(a4, Util.readAddress(dis));
    }


    public void testWriteByteBuffer() throws IOException {
        byte[] buf=new byte[1024], tmp;
        for(int i=0; i < buf.length; i++)
            buf[i]=0;
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeByteBuffer(buf, dos);
        dos.close();
        tmp=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(tmp);
        DataInputStream dis=new DataInputStream(instream);
        byte[] buf2=Util.readByteBuffer(dis);
        assertNotNull(buf2);
        assertEquals(buf.length, buf2.length);
    }



    boolean match(IpAddress a1, IpAddress a2) {
        if(a1 == null && a2 == null)
            return true;
        if(a1 != null)
            return a1.equals(a2);
        else
            return a2.equals(a1);
    }


    public static Test suite() {
        TestSuite s=new TestSuite(UtilTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


