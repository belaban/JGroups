// $Id: UtilTest.java,v 1.11 2006/08/08 07:40:22 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.ChannelException;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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


    public void testPrintBytes() {
        long num;
        String s;

        num=1;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1b", s);

        num=999;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("999b", s);

        num=1000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1KB", s);

        num=1001;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1KB", s);

        num=1010;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1.01KB", s);

        num=1543;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1.54KB", s);

        num=10000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("10KB", s);

        num=150000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("150KB", s);

        num=150023;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("150.02KB", s);

        num=1200000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1.2MB", s);

        num=150000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("150MB", s);

        num=150030000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("150.03MB", s);

        num=1200000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        assertEquals("1.2GB", s);
    }


    public void testObjectToFromByteBuffer() throws Exception {
        byte[] buf;
        IpAddress addr=new IpAddress("localhost", 5000), addr2;
        List list=new ArrayList(), list2;
        list.add("Bela");
        list.add("Jeannette");

        buf=Util.objectToByteBuffer(addr);
        addr2=(IpAddress)Util.objectFromByteBuffer(buf);
        System.out.println("addr=" + addr + ", addr2=" + addr2);
        assertEquals(addr, addr2);

        buf=Util.objectToByteBuffer(list);
        list2=(List)Util.objectFromByteBuffer(buf);
        System.out.println("list=" + list + ", list2=" + list2);
        assertEquals(list, list2);
    }


    public void testWriteStreamable() throws IOException, IllegalAccessException, InstantiationException {
        Message m=new Message(null, null, "Hello");
        ViewId vid=new ViewId(null, 12345);
        ViewId vid2=new ViewId(new IpAddress("127.0.0.1", 5555), 35623);
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeGenericStreamable(m, dos);
        Util.writeGenericStreamable(vid, dos);
        Util.writeGenericStreamable(vid2, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Message m2=(Message)Util.readGenericStreamable(dis);
        ViewId v3=(ViewId)Util.readGenericStreamable(dis);
        ViewId v4=(ViewId)Util.readGenericStreamable(dis);
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
        Util.writeGenericStreamable(vid, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        ViewId v4=(ViewId)Util.readGenericStreamable(dis);
        assertEquals(vid, v4);
    }


    public void testWriteView() throws IOException, IllegalAccessException, InstantiationException {
        ViewId vid=new ViewId(null, 12345);
        Vector members=new Vector();
        View v;
        IpAddress a1=new IpAddress("localhost", 1234);
        IpAddress a2=new IpAddress("127.0.0.1", 4444);
        IpAddress a4=new IpAddress("www.google.com", 7777);
        members.add(a1);
        members.add(a2);
        members.add(a4);
        v=new View(vid, members);

        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeGenericStreamable(v, dos);
        Util.writeStreamable(v, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        View v2=(View)Util.readGenericStreamable(dis);
        assertEquals(v, v2);
        v2=(View)Util.readStreamable(View.class, dis);
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


    public void testPickRandomElement() {
        Vector v=new Vector();
        for(int i=0; i < 10; i++) {
            v.add(new Integer(i));
        }

        Integer el;
        for(int i=0; i < 10000; i++) {
            el=(Integer)Util.pickRandomElement(v);
            assertTrue(el.intValue() >= 0 && el.intValue() < 10);
        }
    }

    public void testAll() {
        ArrayList l=new ArrayList();
        l.add("one"); l.add("two"); l.add("one");
        System.out.println("-- list is " + l);
        assertFalse(Util.all(l, "one"));
        l.remove("two");
        System.out.println("-- list is " + l);
        assertTrue(Util.all(l, "one"));
    }


    public void testParseCommaDelimitedString() {
        String input="1,2,3,4,5,6,7,8,9,10 , 11, 12 ,13";

        List list=Util.parseCommaDelimitedStrings(input);
        System.out.println("list: " + list);
        assertEquals(13, list.size());
        assertEquals("1", list.get(0));
        assertEquals("13", list.get(list.size() -1));
    }


    public void testParseSemicolonDelimitedString() {
        String input="one;two ; three; four ; five;six";
        List list=Util.parseStringList(input, ";");
        System.out.println("list: " + list);
        assertEquals(6, list.size());
        assertEquals("one", list.get(0));
        assertEquals("six", list.get(list.size() -1));
    }


    public void testParseSemicolonDelimitedString2() {
        String input="  myID1::subID1 ; myID2::mySubID2; myID3 ;myID4::blaSubID4";
        List list=Util.parseStringList(input, ";");
        System.out.println("list: " + list);
        assertEquals(4, list.size());
        assertEquals("myID1::subID1", list.get(0));
        assertEquals("myID4::blaSubID4", list.get(list.size() -1));
    }



    public static Test suite() {
        return new TestSuite(UtilTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


