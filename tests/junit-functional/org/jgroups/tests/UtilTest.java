// $Id: UtilTest.java,v 1.4 2007/08/20 09:22:54 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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


    public void testGetProperty() {
        Properties props=new Properties();
        props.setProperty("name", "Bela");
        props.setProperty("key", "val");

        System.setProperty("name", "Michelle");
        System.setProperty("name2", "Nicole");
        String retval;

        retval=Util.getProperty(new String[]{"name", "name2"}, props, "name", false, "Jeannette");
        assertEquals("Michelle", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", false, "Jeannette");
        assertEquals("Nicole", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name"}, props, "name", false, "Jeannette");
        assertEquals("Michelle", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name4"}, props, "name", false, "Jeannette");
        assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", true, "Jeannette");
        assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", true, "Jeannette");
        assertEquals("Jeannette", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", true, null);
        assertNull(retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");
    }


    public void testIgnoreBindAddress() {
        boolean retval;

        retval=Util.isBindAddressPropertyIgnored();
        assertFalse(retval);

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assertTrue(retval);

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true2");
        retval=Util.isBindAddressPropertyIgnored();
        assertFalse(retval);

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "false");
        retval=Util.isBindAddressPropertyIgnored();
        assertFalse(retval);

        System.getProperties().remove(Global.IGNORE_BIND_ADDRESS_PROPERTY);
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "false");
        retval=Util.isBindAddressPropertyIgnored();
        assertFalse(retval);

        System.getProperties().remove(Global.IGNORE_BIND_ADDRESS_PROPERTY);
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assertTrue(retval);


        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true");
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assertTrue(retval);
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
        List<String> list=new ArrayList<String>(), list2;
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

        Object obj=null;
        buf=Util.objectToByteBuffer(obj);
        assertNotNull(buf);
        assertTrue(buf.length > 0);
        obj=Util.objectFromByteBuffer(buf);
        assertNull(obj);

        Object[] values=new Object[]{
                Boolean.TRUE,
                Boolean.FALSE,
                new Byte((byte)22),
                new Byte("2"),
                new Character('5'),
                new Double(3.14),
                new Float(352.3),
                new Integer(100),
                new Long(322649),
                new Short((short)22),
                "Bela Ban"
        };
        for(int i=0; i < values.length; i++) {
            Object value=values[i];
            marshal(value);
        }
    }


    public void testMessageToByteBuffer() throws Exception {
        _testMessage(new Message());
        _testMessage(new Message(null, null, "hello world"));
        _testMessage(new Message(null, new IpAddress("localhost", 5000), null));
        _testMessage(new Message(null, new IpAddress("localhost", 5000), null));
        _testMessage(new Message(null, new IpAddress("localhost", 5000), "bela"));
    }

    private static void _testMessage(Message msg) throws Exception {
        Buffer buf=Util.messageToByteBuffer(msg);
        Message msg2=Util.byteBufferToMessage(buf.getBuf(), buf.getOffset(), buf.getLength());
        assertEquals(msg.getSrc(), msg2.getSrc());
        assertEquals(msg.getDest(), msg2.getDest());
        assertEquals(msg.getLength(), msg2.getLength());
    }


    public void testObjectToByteArrayWithLargeString() throws Exception {
        marshalString(Short.MAX_VALUE );
    }


     public void testObjectToByteArrayWithLargeString2() throws Exception {
        marshalString(Short.MAX_VALUE - 100);
    }


    public void testObjectToByteArrayWithLargeString3() throws Exception {
        marshalString(Short.MAX_VALUE + 1);
    }

    public void testObjectToByteArrayWithLargeString4() throws Exception {
        marshalString(Short.MAX_VALUE + 100);
    }

     public void testObjectToByteArrayWithLargeString5() throws Exception {
        marshalString(Short.MAX_VALUE + 100000);
    }


    private static void marshalString(int size) throws Exception {
        byte[] tmp=new byte[size];
        String str=new String(tmp, 0, tmp.length);
        byte[] retval=Util.objectToByteBuffer(str);
        System.out.println("length=" + retval.length + " bytes");
        String obj=(String)Util.objectFromByteBuffer(retval);
        System.out.println("read " + obj.length() + " string");
    }



    static void marshal(Object obj) throws Exception {
        byte[] buf=Util.objectToByteBuffer(obj);
        assertNotNull(buf);
        assertTrue(buf.length > 0);
        Object obj2=Util.objectFromByteBuffer(buf);
        System.out.println("obj=" + obj + ", obj2=" + obj2 + " (type=" + obj.getClass().getName() + ", length=" + buf.length + " bytes)");
        assertEquals(obj, obj2);
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
        IpAddress a4=new IpAddress("127.0.0.1", 7777);
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

    public static void writeAddress() throws IOException, IllegalAccessException, InstantiationException {
        IpAddress a1=new IpAddress("localhost", 1234);
        IpAddress a2=new IpAddress("127.0.0.1", 4444);
        IpAddress a3=new IpAddress("thishostdoesnexist", 6666);
        IpAddress a4=new IpAddress("127.0.0.1", 7777);

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

    public static void writeNullAddress() throws IOException, IllegalAccessException, InstantiationException {
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


    public void testVariableSubstitution() {
        String val="hello world", replacement;
        replacement=Util.substituteVariable(val);
        assertEquals(val, replacement); // no substitution

        val="my name is ${user.name}";
        replacement=Util.substituteVariable(val);
        assertNotSame(val, replacement);
        assertFalse(val.equals(replacement));

        val="my name is ${user.name} and ${user.name}";
        replacement=Util.substituteVariable(val);
        assertFalse(val.equals(replacement));
        assertEquals(-1, replacement.indexOf("${"));

        val="my name is ${unknown.var:Bela Ban}";
        replacement=Util.substituteVariable(val);
        assertTrue(replacement.contains("Bela Ban"));
        assertEquals(-1, replacement.indexOf("${"));

        val="my name is ${unknown.var}";
        replacement=Util.substituteVariable(val);
        assertTrue(replacement.contains("${"));

        val="here is an invalid ${argument because it doesn't contains a closing bracket";
        try {
            replacement=Util.substituteVariable(val);
            fail("should be an IllegalArgumentException");
        }
        catch(Throwable t) {
            assertEquals(IllegalArgumentException.class, t.getClass());
        }
    }



    public static Test suite() {
        return new TestSuite(UtilTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}


