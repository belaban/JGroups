// $Id: UtilTest.java,v 1.4 2004/10/08 12:07:22 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.ChannelException;
import org.jgroups.ViewId;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Vector;




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
        ViewId vid=new ViewId(null, 12345);
        ViewId vid2=new ViewId(new IpAddress("127.0.0.1", 5555), 35623);
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeStreamable(m, dos);
        Util.writeStreamable(vid, dos);
        Util.writeStreamable(vid2, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Message m2=(Message)Util.readStreamable(dis);
        ViewId v3=(ViewId)Util.readStreamable(dis);
        ViewId v4=(ViewId)Util.readStreamable(dis);
        assertNotNull(m2.getBuffer());
        assertEquals(m.getLength(), m2.getLength());
        assertNotNull(v3);
        assertEquals(vid, v3);
        assertNotNull(v4);
        assertEquals(vid2, v4);
    }

    public void testWriteViewIdWithNullCoordinator() throws IOException, IllegalAccessException, InstantiationException {
        ViewId vid=new ViewId(null, 12345);
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeStreamable(vid, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        ViewId v4=(ViewId)Util.readStreamable(dis);
        assertEquals(vid, v4);
    }


    public void testWriteView() throws IOException, IllegalAccessException, InstantiationException {
        ViewId vid=new ViewId(null, 12345);
        Vector members=new Vector();
        View v;
        IpAddress a1=new IpAddress("localhost", 1234);
        IpAddress a2=new IpAddress("127.0.0.1", 4444);
        IpAddress a3=new IpAddress("thishostdoesnexist", 6666);
        IpAddress a4=new IpAddress("www.google.com", 7777);
        members.add(a1);
        members.add(a2);
        members.add(a3);
        members.add(a4);
        v=new View(vid, members);

        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeStreamable(v, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        View v2=(View)Util.readStreamable(dis);
        assertEquals(v, v2);
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

    public void writeNullAddress() throws IOException, IllegalAccessException, InstantiationException {
        IpAddress a1=null;
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeAddress(a1, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        assertNull(Util.readAddress(dis));
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


    public void testMatch() {
        long[] a={1,2,3};
        long[] b={2,3,4};
        long[] c=null;
        long[] d={1,2,3,4};
        long[] e={1,2,3};

        assertTrue(Util.match(a,a));
        assertFalse(Util.match(a,b));
        assertFalse(Util.match(a,c));
        assertFalse(Util.match(a,d));
        assertTrue(Util.match(a,e));
        assertTrue(Util.match(c,c));
        assertFalse(Util.match(c, a));
    }


    public static Test suite() {
        TestSuite s=new TestSuite(UtilTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


