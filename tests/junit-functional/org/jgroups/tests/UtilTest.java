
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;
import org.jgroups.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;


@Test(groups=Global.FUNCTIONAL)
public class UtilTest {



    public static void testGetProperty() {
        Properties props=new Properties();
        props.setProperty("name", "Bela");
        props.setProperty("key", "val");

        System.setProperty("name", "Michelle");
        System.setProperty("name2", "Nicole");
        String retval;

        retval=Util.getProperty(new String[]{"name", "name2"}, props, "name", false, "Jeannette");
        Assert.assertEquals("Michelle", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", false, "Jeannette");
        Assert.assertEquals("Nicole", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name"}, props, "name", false, "Jeannette");
        Assert.assertEquals("Michelle", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name4"}, props, "name", false, "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", true, "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", true, "Jeannette");
        Assert.assertEquals("Jeannette", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", true, null);
        assert retval == null;
        props.setProperty("name", "Bela"); props.setProperty("key", "val");
    }

    public static void testGetProperty2() {
        String input="foo, bar,  foobar: 1000";
        String result=Util.getProperty(input);
        assert result != null && result.equals("1000");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert result == null;

        System.setProperty("foobar", "900");

        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert result != null && result.equals("900");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert result != null && result.equals("900");


        System.setProperty("bar", "500");
        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert result != null && result.equals("500");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert result != null && result.equals("500");


        System.setProperty("foo", "200");
        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert result != null && result.equals("200");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert result != null && result.equals("200");
    }

    public static void testFlags() {
        final byte ONE   =   1;
        final byte FIVE =   16;
        final byte SEVEN =  64;

        byte flags=0;
        flags=Util.setFlag(flags, ONE);
        assert Util.isFlagSet(flags, ONE);

        flags=0;
        flags=Util.setFlag(flags, (byte)(ONE | SEVEN));
        assert Util.isFlagSet(flags, ONE);
        assert Util.isFlagSet(flags, SEVEN);
        assert !Util.isFlagSet(flags, FIVE);
        flags=Util.clearFlags(flags, ONE);
        assert !Util.isFlagSet(flags, ONE);

        flags=Util.setFlag(flags, FIVE);
        assert Util.isFlagSet(flags, FIVE);
        assert Util.isFlagSet(flags, SEVEN);
    }


    public static void testIgnoreBindAddress() {
        boolean retval;

        retval=Util.isBindAddressPropertyIgnored();
        assert !(retval);

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assert retval;

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true2");
        retval=Util.isBindAddressPropertyIgnored();
        assert !(retval);

        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "false");
        retval=Util.isBindAddressPropertyIgnored();
        assert !(retval);

        System.getProperties().remove(Global.IGNORE_BIND_ADDRESS_PROPERTY);
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "false");
        retval=Util.isBindAddressPropertyIgnored();
        assert !(retval);

        System.getProperties().remove(Global.IGNORE_BIND_ADDRESS_PROPERTY);
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assert retval;


        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY, "true");
        System.setProperty(Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD, "true");
        retval=Util.isBindAddressPropertyIgnored();
        assert retval;
    }


    public static void testPrintBytes() {
        long num;
        String s;

        num=1;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1b", s);

        num=999;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("999b", s);

        num=1000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1KB", s);

        num=1001;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1KB", s);

        num=1010;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1.01KB", s);

        num=1543;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1.54KB", s);

        num=10000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("10KB", s);

        num=150000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150KB", s);

        num=150023;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150.02KB", s);

        num=1200000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1.2MB", s);

        num=150000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150MB", s);

        num=150030000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150.03MB", s);

        num=1200000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1.2GB", s);
    }


    public static void testReadBytes() {
        assert 10 == Util.readBytesInteger("10");
        assert 10 == Util.readBytesInteger("10 ");
        assert 10 == Util.readBytesInteger(" 10");

        assert 1000 == Util.readBytesInteger("1000");
        assert 1000 == Util.readBytesInteger("1kb");
        assert 1000 == Util.readBytesInteger("1 kb");
        assert 1000 == Util.readBytesInteger("1k");
        assert 1000 == Util.readBytesInteger("1KB");
        assert 1000 == Util.readBytesInteger("1 K");
        assert 1000 == Util.readBytesInteger("1K");

        assert 1234 == Util.readBytesInteger("1.234K");

        long M=1000*1000;
        assert M == Util.readBytesLong("1M");
        assert M == Util.readBytesLong("1  M");
        assert M == Util.readBytesLong("1MB");
        assert M == Util.readBytesLong("1 mb");
        assert M == Util.readBytesLong("1m");
        assert M == Util.readBytesLong("1 m");

        M=(long)(25.5 * 1000*1000);
        assert M == Util.readBytesLong("25.5M");
        assert M == Util.readBytesLong("25.5m");
        assert M == Util.readBytesLong("25.5 MB");
        assert M == Util.readBytesLong("25.5 mB");
        assert M == Util.readBytesLong("25.5   m");
        assert M == Util.readBytesLong("25500K");

        M=(long)(1.5 * 1000 * 1000 * 1000);
        assert M == Util.readBytesLong("1.5GB");
        assert M == Util.readBytesLong("1.5gb");
        assert M == Util.readBytesLong("1.5g");
        assert M == Util.readBytesLong("1.5G");
        assert M == Util.readBytesLong("1500m");
        assert M == Util.readBytesLong("1500000K");
        assert M == Util.readBytesLong("1.5 gb");

        double D=3.123456789;
        assert D        == Util.readBytesDouble("3.123456789");
        assert D * 10   == Util.readBytesDouble("31.23456789");
        assert D * 100  == Util.readBytesDouble("312.3456789");
        assert D * 1000 == Util.readBytesDouble("3123.456789");
        assert D * 1000 == Util.readBytesDouble("3.123456789K");
        assert D * 1000000    == Util.readBytesDouble("3.123456789M");
        assert D * 1000000    == Util.readBytesDouble("3123456.789");
                                        
    }

    @SuppressWarnings("unchecked")
    public static void testObjectToFromByteBuffer() throws Exception {
        byte[] buf;
        Address addr=Util.createRandomAddress(), addr2;
        List<String> list=new ArrayList<String>(), list2;
        list.add("Bela");
        list.add("Jeannette");

        buf=Util.objectToByteBuffer(addr);
        addr2=(Address)Util.objectFromByteBuffer(buf);
        System.out.println("addr=" + addr + ", addr2=" + addr2);
        Assert.assertEquals(addr, addr2);

        buf=Util.objectToByteBuffer(list);
        list2=(List<String>)Util.objectFromByteBuffer(buf);
        System.out.println("list=" + list + ", list2=" + list2);
        Assert.assertEquals(list, list2);

        byte[] buffer=new byte[]{'B', 'e', 'l', 'a', ' ', 'B', 'a', 'n'};
        buf=Util.objectToByteBuffer(buffer);

        byte[] buffer2=(byte[])Util.objectFromByteBuffer(buf);
        assert buffer2 != null && buffer.length == buffer2.length;
        assert Arrays.equals(buffer, buffer2);

        Object obj=null;
        buf=Util.objectToByteBuffer(obj);
        assert buf != null;
        assert buf.length > 0;
        obj=Util.objectFromByteBuffer(buf);
        assert obj == null;

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


    public static void testMessageToByteBuffer() throws Exception {
        _testMessage(new Message());
        _testMessage(new Message(null, null, "hello world"));
        _testMessage(new Message(null, Util.createRandomAddress(), null));
        _testMessage(new Message(null, Util.createRandomAddress(), null));
        _testMessage(new Message(null, Util.createRandomAddress(), "bela"));
    }

    private static void _testMessage(Message msg) throws Exception {
        Buffer buf=Util.messageToByteBuffer(msg);
        Message msg2=Util.byteBufferToMessage(buf.getBuf(), buf.getOffset(), buf.getLength());
        Assert.assertEquals(msg.getSrc(), msg2.getSrc());
        Assert.assertEquals(msg.getDest(), msg2.getDest());
        Assert.assertEquals(msg.getLength(), msg2.getLength());
    }


    public static void testObjectToByteArrayWithLargeString() throws Exception {
        marshalString(Short.MAX_VALUE );
    }


     public static void testObjectToByteArrayWithLargeString2() throws Exception {
        marshalString(Short.MAX_VALUE - 100);
    }


    public static void testObjectToByteArrayWithLargeString3() throws Exception {
        marshalString(Short.MAX_VALUE + 1);
    }

    public static void testObjectToByteArrayWithLargeString4() throws Exception {
        marshalString(Short.MAX_VALUE + 100);
    }

     public static void testObjectToByteArrayWithLargeString5() throws Exception {
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
        assert buf != null;
        assert buf.length > 0;
        Object obj2=Util.objectFromByteBuffer(buf);
        System.out.println("obj=" + obj + ", obj2=" + obj2 + " (type=" + obj.getClass().getName() + ", length=" + buf.length + " bytes)");
        Assert.assertEquals(obj, obj2);
    }


    public static void testWriteStreamable() throws IOException, IllegalAccessException, InstantiationException {
        Message m=new Message(null, null, "Hello");
        ViewId vid=new ViewId(null, 12345);
        ViewId vid2=new ViewId(Util.createRandomAddress(), 35623);
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
        assert m2.getBuffer() != null;
        Assert.assertEquals(m.getLength(), m2.getLength());
        assert v3 != null;
        Assert.assertEquals(vid, v3);
        assert v4 != null;
        Assert.assertEquals(vid2, v4);
    }

    public static void testWriteViewIdWithNullCoordinator() throws IOException, IllegalAccessException, InstantiationException {
        ViewId vid=new ViewId(null, 12345);
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeGenericStreamable(vid, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        ViewId v4=(ViewId)Util.readGenericStreamable(dis);
        Assert.assertEquals(vid, v4);
    }


    public static void testWriteView() throws IOException, IllegalAccessException, InstantiationException {
        ViewId vid=new ViewId(null, 12345);
        Vector<Address> members=new Vector<Address>();
        View v;
        Address a1=Util.createRandomAddress();
        Address a2=Util.createRandomAddress();
        Address a4=Util.createRandomAddress();
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
        Assert.assertEquals(v, v2);
        v2=(View)Util.readStreamable(View.class, dis);
        Assert.assertEquals(v, v2);
    }


    public static void testWriteString() throws IOException {
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
        Assert.assertEquals(s1, s3);
        Assert.assertEquals(s2, s4);
    }

    public static void writeAddress() throws IOException, IllegalAccessException, InstantiationException {
        Address a1=Util.createRandomAddress();
        Address a2=Util.createRandomAddress();
        Address a4=Util.createRandomAddress();

        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeAddress(a1, dos);
        Util.writeAddress(a2, dos);
        Util.writeAddress(a4, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);

        Assert.assertEquals(a1, Util.readAddress(dis));
        Assert.assertEquals(a2, Util.readAddress(dis));
        Assert.assertEquals(a4, Util.readAddress(dis));
    }

    public static void writeNullAddress() throws IOException, IllegalAccessException, InstantiationException {
        Address a1=null;
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeAddress(a1, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        assert Util.readAddress(dis) == null;
    }


    public static void testWriteByteBuffer() throws IOException {
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
        assert buf2 != null;
        Assert.assertEquals(buf.length, buf2.length);
    }


    public static void testMatch() {
        long[] a={1,2,3};
        long[] b={2,3,4};
        long[] c=null;
        long[] d={1,2,3,4};
        long[] e={1,2,3};

        assert Util.match(a,a);
        assert !(Util.match(a, b));
        assert !(Util.match(a, c));
        assert !(Util.match(a, d));
        assert Util.match(a,e);
        assert Util.match(c,c);
        assert !(Util.match(c, a));
    }


    public static void testLeftMembers() {
        final Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(), d=Util.createRandomAddress();

        Vector<Address> v1=new Vector<Address>();
        v1.add(a);
        v1.add(b);
        v1.add(c);
        v1.add(d);

        Vector<Address> v2=new Vector<Address>();
        v2.add(c);
        v2.add(d);

        View one=new View(new ViewId(a, 1), v1),
                two=new View(new ViewId(b,2), v2);
        List<Address> left=Util.leftMembers(one, two);
        System.out.println("left = " + left);
        assert left != null;
        assert left.size() == 2;
        assert left.contains(a);
        assert left.contains(b);
    }

    public static void testLeftMembers2() {
        final Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(), d=Util.createRandomAddress();

        Vector<Address> v1=new Vector<Address>();
        v1.add(a);
        v1.add(b);
        v1.add(c);
        v1.add(d);

        Vector<Address> v2=new Vector<Address>();
        v2.add(c);
        v2.add(d);
        v2.add(a);
        v2.add(b);

        View one=new View(new ViewId(a, 1), v1),
                two=new View(new ViewId(b,2), v2);
        List<Address> left=Util.leftMembers(one, two);
        System.out.println("left = " + left);
        assert left != null;
        assert left.isEmpty();
    }


    public static void testNewMembers() {
        final Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(),
                d=Util.createRandomAddress(), e=Util.createRandomAddress();
        List<Address> old=new ArrayList<Address>();
        List<Address> new_list=new ArrayList<Address>();

        old.add(a); old.add(b); old.add(c);
        new_list.add(b);
        new_list.add(a);
        new_list.add(c);
        new_list.add(d);
        new_list.add(e);

        System.out.println("old: " + old);
        System.out.println("new: " + new_list);

        List<Address> new_nodes=Util.newMembers(old, new_list);
        System.out.println("new_nodes = " + new_nodes);
        assert new_nodes.size() == 2 : "list should have d and e";
        assert new_nodes.contains(d);
        assert new_nodes.contains(e);
    }

    public static void testPickRandomElement() {
        Vector<Integer> v=new Vector<Integer>();
        for(int i=0; i < 10; i++) {
            v.add(new Integer(i));
        }

        Integer el;
        for(int i=0; i < 10000; i++) {
            el=(Integer)Util.pickRandomElement(v);
            assert el.intValue() >= 0 && el.intValue() < 10;
        }
    }


    public static void testPickNext() {
        List<Integer> list=new ArrayList<Integer>(10);
        for(int i=0; i < 10; i++)
            list.add(i);
        Integer num=Util.pickNext(list, 5);
        System.out.println("number next to 5: " + num);
        assert num != null;
        assert num.equals(6);

        num=Util.pickNext(list, 9);
        System.out.println("number next to 9: " + num);
        assert num != null;
        assert num.equals(0);

        num=Util.pickNext(list, 11);
        assert num == null;
    }


    public static void testAll() {
        List<String> l=new ArrayList<String>();
        l.add("one"); l.add("two"); l.add("one");
        System.out.println("-- list is " + l);
        assert !(Util.all(l, "one"));
        l.remove("two");
        System.out.println("-- list is " + l);
        assert Util.all(l, "one");
    }


    public static void testParseCommaDelimitedString() {
        String input="1,2,3,4,5,6,7,8,9,10 , 11, 12 ,13";

        List list=Util.parseCommaDelimitedStrings(input);
        System.out.println("list: " + list);
        Assert.assertEquals(13, list.size());
        Assert.assertEquals("1", list.get(0));
        Assert.assertEquals("13", list.get(list.size() - 1));
    }


    public static void testParseSemicolonDelimitedString() {
        String input="one;two ; three; four ; five;six";
        List list=Util.parseStringList(input, ";");
        System.out.println("list: " + list);
        Assert.assertEquals(6, list.size());
        Assert.assertEquals("one", list.get(0));
        Assert.assertEquals("six", list.get(list.size() - 1));
    }


    public static void testParseSemicolonDelimitedString2() {
        String input="  myID1::subID1 ; myID2::mySubID2; myID3 ;myID4::blaSubID4";
        List list=Util.parseStringList(input, ";");
        System.out.println("list: " + list);
        Assert.assertEquals(4, list.size());
        Assert.assertEquals("myID1::subID1", list.get(0));
        Assert.assertEquals("myID4::blaSubID4", list.get(list.size() - 1));
    }


    public static void testReadLine() throws IOException {
        final String input="   hello world\nthis is \r\n just an example\r\nthis is line 2 \r\n";
        String line;
        InputStream in=new BufferedInputStream(new ByteArrayInputStream(input.getBytes()));
        List<String> list=new ArrayList<String>(4);

        for(int i=0; i < 4; i++) {
            line=Util.readLine(in);
            System.out.println("line = \"" + line + "\"");
            list.add(line);
        }

        assert list.size() == 4;
    }


    public static void testVariableSubstitution() {
        String val="hello world", replacement;
        replacement=Util.substituteVariable(val);
        Assert.assertEquals(val, replacement);

        val="my name is ${user.name}";
        replacement=Util.substituteVariable(val);
        Assert.assertNotSame(val, replacement);
        assert !(val.equals(replacement));

        val="my name is ${user.name} and ${user.name}";
        replacement=Util.substituteVariable(val);
        assert !(val.equals(replacement));
        Assert.assertEquals(-1, replacement.indexOf("${"));

        val="my name is ${unknown.var:Bela Ban}";
        replacement=Util.substituteVariable(val);
        assert replacement.contains("Bela Ban");
        Assert.assertEquals(-1, replacement.indexOf("${"));

        val="my name is ${unknown.var}";
        replacement=Util.substituteVariable(val);
        assert replacement.contains("${");

        val="here is an invalid ${argument because it doesn't contains a closing bracket";
        try {
            replacement=Util.substituteVariable(val);
            assert false : "should be an IllegalArgumentException";
        }
        catch(Throwable t) {
            Assert.assertEquals(IllegalArgumentException.class, t.getClass());
        }
    }


    public static void testDetermineMergeParticipantsAndMergeCoords() {
        Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress();
        org.jgroups.util.UUID.add((UUID)a, "A");
        org.jgroups.util.UUID.add((UUID)b, "B");
        org.jgroups.util.UUID.add((UUID)c, "C");

        View v1=Util.createView(b, 1, b, a, c);
        View v2=Util.createView(b, 2, b, c);
        View v3=Util.createView(b, 2, b, c);

        Map<Address,View> map=new HashMap<Address,View>();
        map.put(a, v1); map.put(b, v2); map.put(c, v3);
        StringBuilder sb=new StringBuilder("map:\n");
        for(Map.Entry<Address,View> entry: map.entrySet())
            sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
        System.out.println(sb);

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 2;
        assert merge_participants.contains(a) && merge_participants.contains(b);

        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 1;
        assert merge_coords.contains(b);
    }


    public static void testDetermineMergeParticipantsAndMergeCoords2() {
        Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(), d=Util.createRandomAddress();
        org.jgroups.util.UUID.add((UUID)a, "A");
        org.jgroups.util.UUID.add((UUID)b, "B");
        org.jgroups.util.UUID.add((UUID)c, "C");
        org.jgroups.util.UUID.add((UUID)d, "D");

        View v1=Util.createView(a, 1, a, b);
        View v2=Util.createView(a, 1, a, b);
        View v3=Util.createView(c, 2, c, d);
        View v4=Util.createView(c, 2, c, d);

        Map<Address,View> map=new HashMap<Address,View>();
        map.put(a, v1); map.put(b, v2); map.put(c, v3); map.put(d, v4);

        StringBuilder sb=new StringBuilder("map:\n");
        for(Map.Entry<Address,View> entry: map.entrySet())
            sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
        System.out.println(sb);

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 2;
        assert merge_participants.contains(a) && merge_participants.contains(c);

        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 2;
        assert merge_coords.contains(a) && merge_coords.contains(c);
    }


    public static void testDetermineMergeParticipantsAndMergeCoords3() {
        Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(), d=Util.createRandomAddress();
        org.jgroups.util.UUID.add((UUID)a, "A");
        org.jgroups.util.UUID.add((UUID)b, "B");
        org.jgroups.util.UUID.add((UUID)c, "C");
        org.jgroups.util.UUID.add((UUID)d, "D");

        View v1=Util.createView(a, 1, a, b, c, d);
        View v2=Util.createView(a, 1, a, b, c, d);
        View v3=Util.createView(a, 2, a, b, c, d);
        View v4=Util.createView(a, 3, a, b, c, d);

        Map<Address,View> map=new HashMap<Address,View>();
        map.put(a, v1); map.put(b, v2); map.put(c, v3); map.put(d, v4);

        StringBuilder sb=new StringBuilder("map:\n");
        for(Map.Entry<Address,View> entry: map.entrySet())
            sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
        System.out.println(sb);

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 1;
        assert merge_participants.contains(a);

        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 1;
        assert merge_coords.contains(a);
    }

    public static void testDetermineMergeParticipantsAndMergeCoords4() {
        Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(), d=Util.createRandomAddress();
        org.jgroups.util.UUID.add((UUID)a, "A");
        org.jgroups.util.UUID.add((UUID)b, "B");
        org.jgroups.util.UUID.add((UUID)c, "C");
        org.jgroups.util.UUID.add((UUID)d, "D");

        View v1=Util.createView(a, 1, a, b);
        View v2=Util.createView(c, 1, c, d);

        Map<Address,View> map=new HashMap<Address,View>();
        map.put(a, v1); map.put(b, v1); map.put(d, v2);

        StringBuilder sb=new StringBuilder("map:\n");
        for(Map.Entry<Address,View> entry: map.entrySet())
            sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
        System.out.println(sb);

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 3;
        assert merge_participants.contains(a) && merge_participants.contains(c) && merge_participants.contains(d);

        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 2;
        assert merge_coords.contains(a) && merge_coords.contains(c);
    }


    public static void testAttributeNameToMethodName() {
        _testAttributeNameToMethodName("my_name", "MyName");
        _testAttributeNameToMethodName("bela", "Bela");
        _testAttributeNameToMethodName("oob_max_input_size", "OobMaxInputSize");
        _testAttributeNameToMethodName("a_b_c", "ABC");
        _testAttributeNameToMethodName("aa_bb_cc", "AaBbCc");
        _testAttributeNameToMethodName("i", "I");
        _testAttributeNameToMethodName("tmp", "Tmp");
        _testAttributeNameToMethodName("inet_address_method", "InetAddressMethod");
    }

    public static void testMethodNameToAttributeName() {
        _testMethodNameToAttributeName("setFoo", "foo");
        _testMethodNameToAttributeName("getFoo", "foo");
        _testMethodNameToAttributeName("isLogDiscardMessages", "log_discard_messages");
        _testMethodNameToAttributeName("setOOBMinPoolSize", "oob_min_pool_size");
        _testMethodNameToAttributeName("isOOBThreadPoolEnabled", "oob_thread_pool_enabled");
        _testMethodNameToAttributeName("OOBMinPoolSize", "oob_min_pool_size");
        _testMethodNameToAttributeName("inetAddressMethod", "inet_address_method"); 
    }

    private static void _testMethodNameToAttributeName(String input, String expected_output) {
        String atttr_name=Util.methodNameToAttributeName(input);
        System.out.println("method name=" + input + ", attrname=" + atttr_name + ", expected output=" + expected_output);
        assert atttr_name.equals(expected_output) :
                "method name=" + input + ", attrname=" + atttr_name + ", expected output=" + expected_output;
    }

    private static void _testAttributeNameToMethodName(String input, String expected_output) {
        String method_name=Util.attributeNameToMethodName(input);
        System.out.println("attrname=" + input + ", method name=" + method_name + ", expected output=" + expected_output);
        assert method_name.equals(expected_output) :
                "attrname=" + input + ", method name=" + method_name + ", expected output=" + expected_output;
    }

}


