
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.IpAddressUUID;
import org.jgroups.util.Bits;
import org.jgroups.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.nio.ByteBuffer;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


@Test(groups=Global.FUNCTIONAL)
public class UtilTest {


    public void testShuffle() {
        Integer[] array={1,2,3,4,5};
        System.out.println("array = " + Arrays.toString(array));
        Util.shuffle(array, 0, array.length);
        System.out.println("array = " + Arrays.toString(array));


        array=new Integer[]{1,2,3,4,5};

        // only elements at indices 0-2 can be modified
        for(int i=0; i < 100; i++) {
            Util.shuffle(array, 0, 3);
            System.out.println("array = " + Arrays.toString(array));
            assert array[3] == 4 : String.format("element at index %d was supposed to be %d: %s", 3, 4, Arrays.toString(array));
            assert array[4] == 5 : String.format("element at index %d was supposed to be %d: %s", 4, 5, Arrays.toString(array));
        }
    }

    public void testPermutations() {
        List<Integer> list=Arrays.asList(1,2,3,4);
        List<List<Integer>> permutations=new ArrayList<>(Util.factorial(list.size()));
        Util.permute(list, permutations);

        int expected_permutations=Util.factorial(list.size());
        int permutation_size=permutations.size();
        AtomicInteger count=new AtomicInteger(1);
        permutations.forEach(l -> System.out.printf("%-5d: %s\n", count.getAndIncrement(), l));

        assert expected_permutations == permutation_size:
          String.format("expected %d combinations, got %d\n", expected_permutations, permutation_size);

        Set<List<Integer>> set=new HashSet<>(permutations);
        assert set.size() == permutations.size();
    }


    public static void testGetProperty() {
        Properties props=new Properties();
        props.setProperty("name", "Bela");
        props.setProperty("key", "val");

        System.setProperty("name", "Michelle");
        System.setProperty("name2", "Nicole");

        String retval=Util.getProperty(new String[]{"name", "name2"}, props, "name", "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name"}, props, "name", "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name3", "name4"}, props, "name", "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name", "Jeannette");
        Assert.assertEquals("Bela", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", "Jeannette");
        Assert.assertEquals("Nicole", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");

        retval=Util.getProperty(new String[]{"name2", "name"}, props, "name2", null);
        Assert.assertEquals("Nicole", retval);
        props.setProperty("name", "Bela"); props.setProperty("key", "val");
    }

    public void testOrderedPermutation() {
        List<String> a=Arrays.asList("A1", "A2", "A3");
        List<String> b=Arrays.asList("B1", "B2");
        Collection<List<String>> perms=Util.orderedPermutation(a, b);
        int cnt=0;
        for(List<String> perm: perms) {
            System.out.printf("%d: %s ", ++cnt, perm);
            if(Util.checkOrder(perm, a, b))
                System.out.print("OK\n");
            else
                assert false : String.format("%s is not in correct order (a: %s, b: %s)", perm, a, b);
        }
    }

    public void testOrder() {
        List<Integer> perm=Arrays.asList(1,2,4,3,5);
        List<Integer> l=Arrays.asList(2,3,6);
        assert !Util.checkOrder(perm, l);

        l=Arrays.asList(1,2,5);
        assert Util.checkOrder(perm, l);

        l=Arrays.asList(1,2,3);
        assert Util.checkOrder(perm, l);

        l=Arrays.asList(1,3,2);
        assert !Util.checkOrder(perm, l);
    }



    public static void testGetProperty2() {
        String input="foo, bar,  foobar: 1000";
        String result=Util.getProperty(input);
        assert Objects.equals(result, "1000");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert result == null;

        System.setProperty("foobar", "900");

        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert Objects.equals(result, "900");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert Objects.equals(result, "900");


        System.setProperty("bar", "500");
        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert Objects.equals(result, "500");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert Objects.equals(result, "500");


        System.setProperty("foo", "200");
        input="foo, bar,  foobar: 1000";
        result=Util.getProperty(input);
        assert Objects.equals(result, "200");

        input="foo, bar,  foobar";
        result=Util.getProperty(input);
        assert Objects.equals(result, "200");
    }

    public void testBeginWithDollar() {
        Properties p = new Properties();
        p.put("hello.world", "Hello World");
        String input="${hello.world:foo}";
        String out=Util.substituteVariable(input, p);
        assert out.equals("Hello World");
    }

    public void testReplaceProperties() {
        String input="hello ${my.name:Bela}";

        String out=Util.substituteVariable(input);
        System.out.println("out = " + out);

        assert out.equals("hello Bela");
        Properties props=new Properties();
        props.put("my.name", "Michelle");
        out=Util.substituteVariable(input, props);
        System.out.println("out = " + out);
        assert out.equals("hello Michelle");

        input="hello \\${my.name:Bela}"; // no replacement as the trailing slash prevents this
        out=Util.substituteVariable(input, props);
        System.out.println("out = " + out);

        assert out.equals(input);

        input="\\${escape:bla}";
        out=Util.substituteVariable(input);
        System.out.println("out = " + out);

        assert input.equals(out);


        input="<UDP bind_addr=\"\\${my.bind_addr:127.0.0.1}\" ... />";
        out=Util.substituteVariable(input);
        System.out.println("out = " + out);

        assert input.equals(out);
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

    public void testGetNextHigher() {
        int[][] numbers={
          {0, 1},
          {1,1},
          {2,2},
          {3,4},
          {4,4},
          {5,8},
          {10,16},
          {8000, 8192}
        };

        for(int[] pair: numbers) {
            int input=pair[0];
            int expected=pair[1];
            int actual=Util.getNextHigherPowerOfTwo(input);
            assert expected == actual : "expected " + expected + " but got " + actual + " (input=" + input + ")";
        }
    }

    public void testTossWeightedCoin() {
        boolean rc=Util.tossWeightedCoin(1.0);
        assert true;
        rc=Util.tossWeightedCoin(0.0);
        assert !rc;
    }

    public void testPrintBytes() {
        long num;
        String s;

        char decimal_sep=DecimalFormatSymbols.getInstance().getDecimalSeparator();

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
        Assert.assertEquals("1" + decimal_sep +"00KB", s);

        num=1001;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1"+decimal_sep+"00KB", s);

        num=1010;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1"+decimal_sep+"01KB", s);

        num=1543;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1"+decimal_sep+"54KB", s);

        num=10000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("10"+decimal_sep+"00KB", s);

        num=150000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150"+decimal_sep+"00KB", s);

        num=150023;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150"+decimal_sep+"02KB", s);

        num=1200000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1"+decimal_sep+"20MB", s);

        num=150000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150"+decimal_sep+"00MB", s);

        num=150030000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("150"+decimal_sep+"03MB", s);

        num=1200000000;
        s=Util.printBytes(num);
        System.out.println(num + " is " + s);
        Assert.assertEquals("1"+decimal_sep+"20GB", s);
    }


    public void testProductBiggerThan() {
        boolean rc=Util.productGreaterThan(3, 4, 12);
        assert !rc;
        rc=Util.productGreaterThan(3, 4, 11);
        assert rc;

        long n2=Short.MAX_VALUE/2;
        rc=Util.productGreaterThan(2, n2, Short.MAX_VALUE);
        assert !rc;
        n2++;
        rc=Util.productGreaterThan(2, n2, Short.MAX_VALUE);
        assert rc;

        n2=Long.MAX_VALUE/10;
        rc=Util.productGreaterThan(9, n2, Long.MAX_VALUE);
        assert !rc;
        rc=Util.productGreaterThan(10, n2, Long.MAX_VALUE);
        assert !rc;
        rc=Util.productGreaterThan(11, n2, Long.MAX_VALUE);
        assert rc;
    }

    public void testIsAsciiString() {
        assert Util.isAsciiString("Bela");
        assert !Util.isAsciiString("\u1F601");
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

    public void testSerialization() throws Exception {
        byte[] buf;
        Address addr=Util.createRandomAddress(), addr2;
        List<String> list=new ArrayList<>(), list2;
        list.add("Bela");
        list.add("Jeannette");

        buf=Util.objectToByteBuffer(addr);
        addr2=Util.objectFromByteBuffer(buf);
        System.out.println("addr=" + addr + ", addr2=" + addr2);
        Assert.assertEquals(addr, addr2);

        buf=Util.objectToByteBuffer(list);
        list2=Util.objectFromByteBuffer(buf);
        System.out.println("list=" + list + ", list2=" + list2);
        Assert.assertEquals(list, list2);

        byte[] buffer={'B', 'e', 'l', 'a', ' ', 'B', 'a', 'n'};
        buf=Util.objectToByteBuffer(buffer);

        byte[] buffer2=Util.objectFromByteBuffer(buf);
        assert buffer2 != null && buffer.length == buffer2.length;
        assert Arrays.equals(buffer, buffer2);

        Object obj=null;
        buf=Util.objectToByteBuffer(obj);
        assert buf != null;
        assert buf.length > 0;
        obj=Util.objectFromByteBuffer(buf);
        assert obj == null;

        Object[] values={
          Boolean.TRUE,
          true,
          false,
          Boolean.FALSE,
          (byte)22,
          Byte.valueOf("2"),
          '5',
          3.14,
          352.3f,
          0,
          100,
          322649,
          Integer.MAX_VALUE,
          Integer.MIN_VALUE,
          0L,
          322649L,
          Long.MAX_VALUE-50,
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          (short)22,
          Short.MAX_VALUE,
          Short.MIN_VALUE,
          "Bela Ban",
          "\u1F601", // multibyte string
          new byte[]{'H', 'e', 'l', 'l', 'o'},
          Util.generateArray(1024)
        };
        System.out.print("\n ------------ objectToByteBuffer() ------------\n");
        for(int i=0; i < values.length; i++) {
            Object value=values[i];
            objectToByteBuffer(value);
        }
        System.out.print("\n ------------ objectToBuffer() ------------\n");
        for(int i=0; i < values.length; i++) {
            Object value=values[i];
            objectToBuffer(value);
        }
        System.out.print("\n ------------ objectToStream() ------------\n");
        for(int i=0; i < values.length; i++) {
            Object value=values[i];
            objectToStream(value);
        }
    }



    public static void testMessageToByteBuffer() throws Exception {
        _testMessage(new Message());
        _testMessage(new Message(null, "hello world"));
        _testMessage(new Message(null).src(Util.createRandomAddress()));
        _testMessage(new Message(null).src(Util.createRandomAddress()));
        _testMessage(new Message(null, "bela").src(Util.createRandomAddress()));
    }

    private static void _testMessage(Message msg) throws Exception {
        Buffer buf=Util.messageToByteBuffer(msg);
        Message msg2=Util.byteBufferToMessage(buf.getBuf(), buf.getOffset(), buf.getLength());
        Assert.assertEquals(msg.getSrc(), msg2.getSrc());
        Assert.assertEquals(msg.getDest(), msg2.getDest());
        Assert.assertEquals(msg.getLength(), msg2.getLength());
    }

    public void testStringMarshalling() throws Exception {
        byte[] tmp={'B', 'e', 'l', 'a'};
        String str=new String(tmp);
        byte[] buf=Util.objectToByteBuffer(str);
        String str2=Util.objectFromByteBuffer(buf);
        assert str.equals(str2);
        tmp[1]='a';
        str2=Util.objectFromByteBuffer(buf);
        assert str.equals(str2);
    }

    public void testStringMarshalling2() throws Exception {
        String first="Bela";
        long val=322649;
        String last="Ban";
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(64);
        Util.objectToStream(first, out);
        Util.objectToStream(val, out);
        Util.objectToStream(last, out);

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        String first2=Util.objectFromStream(in);
        long val2=Util.objectFromStream(in);
        String last2=Util.objectFromStream(in);

        assert first.equals(first2);
        assert val == val2;
        assert last.equals(last2);
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


    public void testExceptionToStream() throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512, true);
        Throwable cause=new IllegalArgumentException("this is highly illegal!");
        NullPointerException ex=new NullPointerException("boom");
        ex.initCause(cause);
        int stack_len=ex.getStackTrace().length;

        Util.exceptionToStream(ex, out);

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        Throwable new_ex=Util.exceptionFromStream(in);
        int new_stack_len=new_ex.getStackTrace().length;


        assert new_ex instanceof NullPointerException;
        assert new_ex.getMessage().equals("boom");
        assert new_ex.getStackTrace().length > 0;
        assert new_stack_len == stack_len;

        Throwable new_cause=new_ex.getCause();
        assert new_cause instanceof IllegalArgumentException;
        assert new_cause.getMessage().startsWith("this is highly");
    }


    public void testExceptionWithStackTrace() throws Exception {
        Throwable ex=foo();
        System.out.println("ex = " + ex);

        int stack_trace_len=ex.getStackTrace().length;

        Buffer buf=Util.objectToBuffer(ex);

        Throwable ex2=Util.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
        System.out.println("ex2 = " + ex2);

        int stack_trace_len2=ex2.getStackTrace().length;
        assert stack_trace_len == stack_trace_len2;

        ex=new InvocationTargetException(ex, "target");
        System.out.println("ex = " + ex);

        stack_trace_len=ex.getStackTrace().length;

        buf=Util.objectToBuffer(ex);

        ex2=Util.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
        System.out.println("ex2 = " + ex2);

        stack_trace_len2=ex2.getStackTrace().length;
        assert stack_trace_len == stack_trace_len2;
    }


    protected static Throwable foo() throws Exception {
        try {
            bar();
            return null;
        }
        catch(Throwable throwable) {
            throwable.printStackTrace(new PrintStream(new FileOutputStream("/dev/null")));
            return throwable;
        }
    }

    protected static boolean bar() throws Throwable {
        return foobar();
    }

    protected static boolean foobar() throws Throwable {
        Throwable ex=new NullPointerException("boom");
        ex.initCause(new IllegalArgumentException("illegal"));
        throw ex;
    }


    protected static class MyNioReceiver extends org.jgroups.blocks.cs.ReceiverAdapter {
        protected String name;

        @Override
        public void receive(Address sender, byte[] buf, int offset, int length) {
            name=new String(buf, offset, length);
        }

        @Override
        public void receive(Address sender, ByteBuffer buf) {
            Util.bufferToArray(sender, buf, this);
        }
    }

    public void testCombine() {
        String[] arr1={"Hello", "world"};
        String[] arr2={"from", "Bela"};
        String[] arr3={"Ban"};

        String[] combined=Util.combine(arr1, arr2, arr3);
        assert combined.length == 5;
        assert combined[0].equals("Hello");
        assert combined[2].equals("from");
    }

    public void testBufferToArray() {
        // test heap based ByteBuffer:
        String hello="hello";
        byte[] buffer=hello.getBytes();
        ByteBuffer buf=ByteBuffer.allocate(50).putInt(322649).put(buffer);
        buf.flip(); // for java 8 compatibility

        buf.getInt();
        MyNioReceiver receiver=new MyNioReceiver();
        receiver.receive(null, buf);
        assert receiver.name.equals(hello);

        // test direct ByteBuffer:
        buf=ByteBuffer.allocateDirect(50).putInt(322649).put(buffer);
        buf.flip();
        buf.getInt();
        receiver.receive(null, buf);
        assert receiver.name.equals(hello);
    }

    private static void marshalString(int size) throws Exception {
        byte[] tmp=new byte[size];
        String str=new String(tmp, 0, tmp.length);
        byte[] retval=Util.objectToByteBuffer(str);
        System.out.println("length=" + retval.length + " bytes");
        String obj=Util.objectFromByteBuffer(retval);
        System.out.println("read " + obj.length() + " string");
    }



    static void objectToByteBuffer(Object obj) throws Exception {
        byte[] buf=Util.objectToByteBuffer(obj);
        assert buf != null;
        assert buf.length > 0;
        Object obj2=Util.objectFromByteBuffer(buf);
        System.out.println("obj=" + obj + ", obj2=" + obj2 + " (type=" + obj.getClass().getName() + ", length=" + buf.length + " bytes)");
        Assert.assertEquals(obj, obj2);

        if(obj instanceof Integer) { // test compressed ints and longs
            buf=new byte[10];
            Bits.writeIntCompressed((int)obj, buf, 0);
            obj2=Bits.readIntCompressed(buf, 0);
            assert obj.equals(obj2);
        }
        if(obj instanceof Long) { // test compressed ints and longs
            buf=new byte[10];
            Bits.writeLongCompressed((long)obj, buf, 0);
            obj2=Bits.readLongCompressed(buf, 0);
            assert obj.equals(obj2);
        }
    }


    static void objectToBuffer(Object obj) throws Exception {
        Buffer buf=Util.objectToBuffer(obj);
        assert buf != null;
        assert buf.getLength() > 0;
        Object obj2=Util.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
        System.out.println("obj=" + obj + ", obj2=" + obj2 + " (type=" + obj.getClass().getName() + ", length=" + buf.getLength() + " bytes)");
        Assert.assertEquals(obj, obj2);
    }

    static void objectToStream(Object obj) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(128);
        Util.objectToStream(obj, out);
        assert out.position() > 0;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());

        Object obj2=Util.objectFromStream(in);
        System.out.println("obj=" + obj + ", obj2=" + obj2 + " (type=" + obj.getClass().getName() + ", length=" + out.position() + " bytes)");
        Assert.assertEquals(obj, obj2);
    }

    public static void testWriteStreamable() throws Exception {
        Message m=new Message(null, "Hello");
        ViewId vid2=new ViewId(Util.createRandomAddress(), 35623);
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeGenericStreamable(m, dos);
        Util.writeGenericStreamable(vid2, dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Message m2=Util.readGenericStreamable(dis);
        ViewId v3=Util.readGenericStreamable(dis);
        assert m2.getBuffer() != null;
        Assert.assertEquals(m.getLength(), m2.getLength());
        assert v3 != null;
    }


    public static void testWriteView() throws Exception {
        List<Address> members=new ArrayList<>();
        View v;
        Address a1=Util.createRandomAddress();
        Address a2=Util.createRandomAddress();
        Address a4=Util.createRandomAddress();
        ViewId vid=new ViewId(a1, 12345);
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
        View v2=Util.readGenericStreamable(dis);
        Assert.assertEquals(v, v2);
        v2=Util.readStreamable(View::new, dis);
        Assert.assertEquals(v, v2);
    }


    public static void testWriteString() throws Exception {
        String s1="Bela Ban", s2="Michelle Ban";
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Bits.writeString(s1,dos);
        Bits.writeString(s2,dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        String s3=Bits.readString(dis);
        String s4=Bits.readString(dis);
        Assert.assertEquals(s1, s3);
        Assert.assertEquals(s2, s4);
    }

    public void testWriteAddress() throws Exception {
        Address a1=Util.createRandomAddress();
        Address a2=Util.createRandomAddress();
        Address a4=Util.createRandomAddress();
        Address a5=new IpAddress("127.0.0.1", 5555);
        Address a6=new IpAddressUUID("127.0.0.1", 5555);

        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        Util.writeAddress(a1, dos);
        Util.writeAddress(a2, dos);
        Util.writeAddress(a4, dos);
        Util.writeAddress(a5, dos);
        Util.writeAddress(a6, dos);

        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);

        Assert.assertEquals(a1, Util.readAddress(dis));
        Assert.assertEquals(a2, Util.readAddress(dis));
        Assert.assertEquals(a4, Util.readAddress(dis));

        Address tmp=Util.readAddress(dis);
        assert a5.equals(tmp);
        tmp=Util.readAddress(dis);
        assert a6.equals(tmp);
    }

    public static void testWriteNullAddress() throws Exception {
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


    public static void testWriteByteBuffer() throws Exception {
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

    public static void testWriteAndReadStreamableArray() throws Exception {
        Message[] msgs={
          new Message(null, "hello world").setFlag(Message.Flag.OOB, Message.Flag.NO_RELIABILITY),
          new Message(Util.createRandomAddress("dest"), "bela ban"),
          new Message(Util.createRandomAddress("dest"), "hello world again").src(Util.createRandomAddress("src"))
            .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK)
        };

        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(256);
        Util.write(msgs, out);

        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        Message[] tmp=Util.read(Message.class, in);
        for(int i=0; i < msgs.length; i++) {
            if(msgs[i].dest() == null)
                assert tmp[i].dest() == null;
            else
                assert(msgs[i].dest().equals(tmp[i].dest()));
            assert msgs[i].getLength() == tmp[i].getLength();
            assert msgs[i].getObject().equals(tmp[i].getObject());
        }
    }

    public void testRandom() {
        List<Long> list=LongStream.rangeClosed(1,10).boxed().collect(Collectors.toList());
        for(int i=0; i < 1_000_000; i++) {
            long random=Util.random(10);
            assert random >= 1 && random <= 10: String.format("random is %d", random);
            if(list.remove(random) && list.isEmpty())
                break;
        }
        assert list.isEmpty();

        long random=Util.random(1);
        assert random == 1;

        random=Util.random(0);
        assert random == 1;
    }


    public static void testChanged() {
        Address a=null, b=null;
        assert !Util.different(a,b);

        a=Util.createRandomAddress("A");
        assert Util.different(a,b);
        assert Util.different(b,a);

        b=Util.createRandomAddress("B");
        assert Util.different(a,b);
        assert Util.different(b,a);

        b=a;
        assert !Util.different(a,b);
        assert !Util.different(b,a);
        a=null;
        assert Util.different(a,b);
        assert Util.different(b,a);
    }


    public static void testAllEqual() {
        Address[] mbrs=Util.createRandomAddresses(5);
        View[] views={View.create(mbrs[0], 1, mbrs), View.create(mbrs[0], 1, mbrs), View.create(mbrs[0], 1, mbrs)};

        boolean same=Util.allEqual(Arrays.asList(views));
        System.out.println("views=" + Arrays.toString(views) + ", same = " + same);
        assert same;

        views=new View[]{View.create(mbrs[0], 1, mbrs), View.create(mbrs[0], 2, mbrs), View.create(mbrs[0], 1, mbrs)};
        same=Util.allEqual(Arrays.asList(views));
        System.out.println("views=" + Arrays.toString(views) + ", same = " + same);
        assert !same;

        views=new View[]{View.create(mbrs[1], 1, mbrs), View.create(mbrs[0], 1, mbrs), View.create(mbrs[0], 1, mbrs)};
        same=Util.allEqual(Arrays.asList(views));
        System.out.println("views=" + Arrays.toString(views) + ", same = " + same);
        assert !same;
    }





    public static void testNewMembers() {
        final Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress(),
                d=Util.createRandomAddress(), e=Util.createRandomAddress();
        List<Address> old=new ArrayList<>();
        List<Address> new_list=new ArrayList<>();

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

    public void testPickRandomElement() {
        List<Integer> v=new ArrayList<>();
        for(int i=0; i < 10; i++) {
            v.add(i);
        }
        for(int i=0; i < 10000; i++) {
            Integer el=Util.pickRandomElement(v);
            assert el >= 0 && el < 10;
        }
    }

    public void testPickRandomElement2() {
        List<Integer> list=IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        for(int i=0; i < 1_000_000; i++) {
            Integer el=Util.pickRandomElement(list);
            boolean rc=list.remove(el);
            assert rc;
            if(list.isEmpty())
                break;
        }
        assert list.isEmpty();
    }

    public void testPickRandomElementFromSet() {
        Set<Integer> set=new TreeSet<>();
        for(int i=10; i > 0; i--)
            set.add(i);

        for(int i=0; i < 100; i++) {
            Integer n=Util.pickRandomElement(set);
            assert n >= 0 && n <= 10 : "n: " + n;
        }
    }


    public void testPickNext() {
        List<Integer> list=new ArrayList<>(10);
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


    public static void testPickNextN() {
        List<Integer> list=Arrays.asList(1,2,3,4);
        List<Integer> result=Util.pickNext(list, 0, 1);
        assert result.isEmpty();

        result=Util.pickNext(list, 1, 1);
        System.out.println("result = " + result);
        assert result.size() == 1;
        assert result.contains(2);

        result=Util.pickNext(list, 3, 2);
        System.out.println("result = " + result);
        assert result.size() == 2;
        assert result.contains(4) && result.contains(1);

        result=Util.pickNext(list, 4, 5);
        System.out.println("result = " + result);
        assert result.size() == 3;
        assert result.contains(1) && result.contains(2) && result.contains(3);
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


    public void testReadLine() throws IOException {
        final String input="   hello world\nthis is \r\n just an example\r\nthis is line 2 \r\n";
        String line;
        InputStream in=new BufferedInputStream(new ByteArrayInputStream(input.getBytes()));
        List<String> list=new ArrayList<>(4);

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
        org.jgroups.util.NameCache.add(a, "A");
        org.jgroups.util.NameCache.add(b, "B");
        org.jgroups.util.NameCache.add(c, "C");

        View v1=View.create(b, 1, b, a, c);
        View v2=View.create(b, 2, b, c);
        View v3=View.create(b, 2, b, c);

        Map<Address,View> map=new HashMap<>();
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
        org.jgroups.util.NameCache.add(a, "A");
        org.jgroups.util.NameCache.add(b, "B");
        org.jgroups.util.NameCache.add(c, "C");
        org.jgroups.util.NameCache.add(d, "D");

        View v1=View.create(a, 1, a, b);
        View v2=View.create(a, 1, a, b);
        View v3=View.create(c, 2, c, d);
        View v4=View.create(c, 2, c, d);

        Map<Address,View> map=new HashMap<>();
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
        org.jgroups.util.NameCache.add(a, "A");
        org.jgroups.util.NameCache.add(b, "B");
        org.jgroups.util.NameCache.add(c, "C");
        org.jgroups.util.NameCache.add(d, "D");

        View v1=View.create(a, 1, a, b, c, d);
        View v2=View.create(a, 1, a, b, c, d);
        View v3=View.create(a, 2, a, b, c, d);
        View v4=View.create(a, 3, a, b, c, d);

        Map<Address,View> map=new HashMap<>();
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
        org.jgroups.util.NameCache.add(a, "A");
        org.jgroups.util.NameCache.add(b, "B");
        org.jgroups.util.NameCache.add(c, "C");
        org.jgroups.util.NameCache.add(d, "D");

        View v1=View.create(a, 1, a, b);
        View v2=View.create(c, 1, c, d);

        Map<Address,View> map=new HashMap<>();
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
        _testMethodNameToAttributeName("getAge", "age");
        _testMethodNameToAttributeName("get", "get");
        _testMethodNameToAttributeName("set", "set");
        _testMethodNameToAttributeName("is", "is");
        _testMethodNameToAttributeName("age", "age");
        _testMethodNameToAttributeName("lastName", "last_name");
    }

    public void testWaitUntilAllChannelsHaveSameView() throws Exception {
        JChannel a=null, b=null;

        try {
            a=new JChannel(Util.getTestStack()).name("A").connect("demo");
            b=new JChannel(Util.getTestStack()).name("B").connect("demo");
            Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);

            try {
                Util.waitUntilAllChannelsHaveSameView(2000, 500, a);
                assert false;
            }
            catch(Exception ex) {
                System.out.printf("threw exception as expected: %s\n", ex);
            }
        }
        finally {
            Util.close(b,a);
        }
    }

    public void testGetLoopback() throws UnknownHostException {
        InetAddress addr=Util.getLoopback(StackType.IPv4);
        assert addr instanceof Inet4Address && addr.equals(InetAddress.getByName("127.0.0.1"));
        addr=Util.getLoopback(StackType.IPv6);
        assert addr instanceof Inet6Address && addr.equals(InetAddress.getByName("::1"));
    }

    public void testGetNonLoopbackAddress() throws SocketException {
        InetAddress addr=Util.getNonLoopbackAddress(StackType.IPv4);
        assert addr == null || addr instanceof Inet4Address;
        addr=Util.getNonLoopbackAddress(StackType.IPv6);
        assert addr == null || addr instanceof Inet6Address;
    }

    public void testGetByName() throws UnknownHostException {
        InetAddress addr=Util.getByName("127.0.0.1", StackType.IPv4);
        assert addr instanceof Inet4Address;
        addr=Util.getByName("127.0.0.1", StackType.IPv6);
        assert addr == null;
        addr=Util.getByName("localhost", StackType.IPv4);
        assert addr instanceof Inet4Address;
        addr=Util.getByName("localhost", StackType.IPv6);
        assert addr instanceof Inet6Address;
    }


    public void testGetAddressByScope() throws SocketException {
        for(Util.AddressScope scope: Util.AddressScope.values()) {
            for(StackType type: StackType.values()) {
                InetAddress addr=Util.getAddress(scope, type);
                System.out.printf("%s %s -> %s\n", type, scope, addr);
                assert addr == null || addr.getClass() == (type == StackType.IPv6? Inet6Address.class : Inet4Address.class);
            }
        }
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


