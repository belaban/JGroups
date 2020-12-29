
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Class to test fragmentation. It uses a minimal stack with different fragmentation protocols.
 * Sends NUM_MSGS with MSG_SIZE size down the stack, they should be received as well.
 *
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="fragProvider")
public class FragTest {
    public static final long NUM_MSGS  =   1000;
    public static final int  MSG_SIZE  = 100000;
    public static final int  FRAG_SIZE =  24000;

    protected JChannel            a, b;
    protected MyReceiver<Message> r1=new org.jgroups.util.MyReceiver<Message>().rawMsgs(true),
                                  r2=new org.jgroups.util.MyReceiver<Message>().rawMsgs(true);
    protected static final byte[] array=Util.generateArray(FRAG_SIZE*2);

    @DataProvider
    static Object[][] fragProvider() {
        return new Object[][] {
          {FRAG.class},
          {FRAG2.class},
          {FRAG3.class},
          {FRAG4.class}
        };
    }

    @Test(enabled=false)
    protected void setup(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz, false);
    }

    @Test(enabled=false)
    protected void setup(Class<? extends Fragmentation> frag_clazz, boolean use_encrypt) throws Exception {
        a=createChannel("A", frag_clazz, use_encrypt).connect("FragTest");
        b=createChannel("B", frag_clazz, use_encrypt).connect("FragTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);
        a.setReceiver(r1);
        b.setReceiver(r2);
    }

    @AfterMethod protected void destroy() {Util.close(b, a); r1.reset(); r2.reset();}



    public void testRegularMessages(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message big_msg=createMessage(b.getAddress(), MSG_SIZE);
            a.send(big_msg);
        }
        System.out.println("-- done sending");
        Util.waitUntil(5000, 500, () -> r2.size() == NUM_MSGS,
                       () -> String.format("expected %d messages, but received %d", NUM_MSGS, r2.size()));
    }


    public void testMessagesWithOffsets(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        byte[] big_buffer=new byte[(int)(MSG_SIZE * NUM_MSGS)];
        int offset=0;

        for(int i=1; i <= NUM_MSGS; i++) {
            Message big_msg=new BytesMessage(b.getAddress(), big_buffer, offset, MSG_SIZE);
            a.send(big_msg);
            offset+=MSG_SIZE;
        }
        System.out.println("-- done sending");
        Util.waitUntil(5000, 500, () -> r2.size() == NUM_MSGS,
                       () -> String.format("expected %d messages, but received %d", NUM_MSGS, r2.size()));
    }

    /**
     * Tests potential ordering violation by sending small, unfragmented messages, followed by a large message
     * which generates 3 fragments, followed by a final small message. Verifies that the message assembled from the
     * 3 fragments is in the right place and not at the end. JIRA=https://issues.jboss.org/browse/JGRP-1648
     */
    public void testMessageOrdering(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        Fragmentation frag=a.getProtocolStack().findProtocol(Fragmentation.class);
        frag.setFragSize(5000);

        Address dest=b.getAddress();
        Message first=new BytesMessage(dest, new Payload(1, 10));
        Message big=new BytesMessage(dest, new Payload(2, 12000)); // frag_size is 5000, so FRAG{2} will create 3 fragments
        Message last=new BytesMessage(dest, new Payload(3, 10));

        a.send(first);
        a.send(big);
        a.send(last);

        List<Message> list=r2.list();
        Util.waitUntil(3000, 500, () -> r2.size() == 3);
        System.out.println("list = " + list);

        // assert that the ordering is [1 2 3], *not* [1 3 2]
        for(int i=0; i < list.size(); i++) {
            int seqno=((Payload)list.get(i).getObject()).seqno;
            assert seqno == i+1 : "element at index " + i + " is " + seqno + ", was supposed to be " + (i+1);
        }
    }

    /* Tests https://issues.jboss.org/browse/JGRP-1973 */
    public void testFragCorruption(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        final String message="this message is supposed to get fragmented by A and defragmented by B";
        byte[] buf=message.getBytes();
        Message msg=new BytesMessage(b.getAddress(), buf).setFlag(Message.Flag.OOB);
        List<Message> list=r2.list();
        a.send(msg);
        Util.waitUntil(5000, 500, () -> list.size() == 1);
        Message m=list.get(0);
        String s=new String(m.getArray(), m.getOffset(), m.getLength());
        assert s.equals(message) : String.format("expected \"%s\" but received \"%s\"\n", message, s);
        System.out.printf("received \"%s\"\n", s);
    }


    public void testEmptyMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        Message m1=new EmptyMessage(null), m2=new EmptyMessage(b.getAddress());
        send(m1, m2);
        assertForAllMessages(m -> m.getLength() == 0);
    }


    public void testBytesMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        Message m1=new BytesMessage(null, array), m2=new BytesMessage(b.getAddress(), array);
        send(m1, m2);
        assertForAllMessages(m -> m.getLength() == array.length);
        assertForAllMessages(m -> Util.verifyArray(m.getArray()));
    }


    public void testObjectMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        MySizeData obj=new MySizeData(322649, array);
        Message m1=new ObjectMessage(null, obj), m2=new ObjectMessage(b.getAddress(), obj);
        send(m1, m2);
        assertForAllMessages(m -> {
            MySizeData data=m.getObject();
            return data.equals(obj) && Util.verifyArray(data.array());
        });
    }


    public void testObjectMessageWithCompression(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        for(JChannel ch: Arrays.asList(a,b)) {
            COMPRESS c=new COMPRESS();
            ch.getProtocolStack().insertProtocol(c, ProtocolStack.Position.BELOW, NAKACK2.class);
            c.init();
        }

        MySizeData obj=new MySizeData(322649, array);
        Message m1=new ObjectMessage(null, obj), m2=new ObjectMessage(b.getAddress(), obj);
        send(m1, m2);
        assertForAllMessages(m -> {
            MySizeData data=m.getObject();
            return data.equals(obj) && Util.verifyArray(data.array());
        });
    }

    public void testObjectMessageWithEncryption(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz, true);
        MySizeData obj=new MySizeData(322649, array);
        Message m1=new ObjectMessage(null, obj), m2=new ObjectMessage(b.getAddress(), obj);
        send(m1, m2);
        assertForAllMessages(m -> {
            MySizeData data=m.getObject();
            return data.equals(obj) && Util.verifyArray(data.array());
        });
    }

    public void testObjectMessage3(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        MyData obj=new MyData(322649, array);
        Message m1=new ObjectMessage(null, obj), m2=new ObjectMessage(b.getAddress(), obj);
        send(m1, m2);
        assertForAllMessages(m -> {
            MyData data=m.getObject();
            return data.equals(obj) && Util.verifyArray(data.array());
        });
    }

    public void testObjectMessage4(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        Person p=new Person("Bela Ban", 53, array);
        Message m1=new ObjectMessage(null, p), m2=new ObjectMessage(b.getAddress(), p);
        send(m1, m2);
        assertForAllMessages(m -> {
            Person p2=m.getObject();
            return p2.name.equals("Bela Ban") && p2.age == p.age && Util.verifyArray(p.buf);
        });
    }

    public void testNioHeapMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        NioMessage m1=new NioMessage(null, ByteBuffer.wrap(array)),
          m2=new NioMessage(b.getAddress(), ByteBuffer.wrap(array));
        send(m1, m2);
        assertForAllMessages(m -> Util.verifyByteBuffer(((NioMessage)m).getBuf()));
    }

    public void testNioDirectMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        NioMessage m1=new NioMessage(null, Util.wrapDirect(array)),
          m2=new NioMessage(b.getAddress(), Util.wrapDirect(array));
        send(m1, m2);
        assertForAllMessages(m -> Util.verifyByteBuffer(((NioMessage)m).getBuf()));
    }

    public void testCompositeMessage(Class<? extends Fragmentation> frag_clazz) throws Exception {
        setup(frag_clazz);
        CompositeMessage m1=new CompositeMessage(null, new EmptyMessage(null));
        IntStream.of(10000, 15000, 5000).forEach(n -> m1.add(new BytesMessage(null, new byte[n])));
        Person p=new Person("Bela Ban", 53, array);
        m1.add(new ObjectMessage(null, p));
        m1.add(new NioMessage(null, ByteBuffer.wrap(array)));
        m1.add(new NioMessage(null, Util.wrapDirect(array)).useDirectMemory(false));

        CompositeMessage m2=new CompositeMessage(b.getAddress(), new EmptyMessage(b.getAddress()));
        send(m1, m2);
    }



    protected static JChannel createChannel(String name, Class<? extends Fragmentation> clazz,
                                            boolean use_encr) throws Exception {
        Fragmentation frag_prot=clazz.getDeclaredConstructor().newInstance();
        frag_prot.setFragSize(FRAG_SIZE);
        ASYM_ENCRYPT e=use_encr? new ASYM_ENCRYPT().setChangeKeyOnLeave(false).setUseExternalKeyExchange(false) : null;
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            e,
                            new NAKACK2().useMcastXmit(false),
                            new UNICAST3(),
                            new STABLE().setMaxBytes(50000),
                            new GMS().setJoinTimeout(500).printLocalAddress(false),
                            new UFC(),
                            new MFC(),
                            frag_prot)
          .name(name);
    }

    protected static Message createMessage(Address dest, int size) {
        return new BytesMessage(dest, new byte[size]);
    }

    protected void send(Message mcast, Message ucast) throws Exception {
        a.send(mcast); // from A --> all
        a.send(ucast); // from A --> B

        // wait until A and B have received mcast, and until B has received ucast
        Util.waitUntil(100000, 500, () -> r1.size() == 1 && r2.size() == 2);
        System.out.printf("A: %s\nB: %s\nB: %s\n",
                          String.format("%s %s", r1.list().get(0).getClass().getSimpleName(), r1.list().get(0)),
                          String.format("%s %s", r2.list().get(0).getClass().getSimpleName(), r2.list().get(0)),
                          String.format("%s %s", r2.list().get(1).getClass().getSimpleName(), r2.list().get(1)));
        if(mcast != null && ucast != null)
            assertForAllMessages(m -> m.getClass().equals(mcast.getClass()) && m.getClass().equals(ucast.getClass()));

        assertForAllMessages(m -> m.getSrc().equals(a.getAddress()));
        assert r1.list().get(0).getDest() == null;
        // one dest must be null and the other B:
        assert Stream.of(r2.list()).flatMap(Collection::stream).anyMatch(m -> m.getDest() == null);
        assert Stream.of(r2.list()).flatMap(Collection::stream).anyMatch(m -> Objects.equals(m.getDest(), b.getAddress()));
    }

    protected void assertForAllMessages(Predicate<Message> p) {
        assert Stream.of(r1.list(), r2.list()).flatMap(Collection::stream).allMatch(p);
    }




    protected static class Payload implements Serializable {
        private static final long serialVersionUID=-1989899280425578506L;
        protected int    seqno;
        protected byte[] buffer;

        protected Payload(int seqno, int size) {
            this.seqno=seqno;
            this.buffer=new byte[size];
        }
    }

    public static class MyData implements Streamable {
          protected int    num;
          protected byte[] arr;

          public MyData() {}

          public MyData(int num, byte[] buf) {
              this.num=num;
              this.arr=buf;
          }

          public byte[] array() {return arr;}

          public boolean equals(Object obj) {
              MyData d=(MyData)obj;
              return num == d.num && arr.length == d.arr.length;
          }

          public String toString() {
              return String.format("num=%d, data: %d bytes", num, arr != null? arr.length : 0);
          }

          public void writeTo(DataOutput out) throws IOException {
              out.writeInt(num);
              out.writeInt(arr != null? arr.length : 0);
              if(arr != null)
                  out.write(arr, 0, arr.length);
          }

          public void readFrom(DataInput in) throws IOException {
              num=in.readInt();
              int len=in.readInt();
              if(len > 0) {
                  arr=new byte[len];
                  in.readFully(arr);
              }
          }
      }


    public static class MySizeData extends MyData implements SizeStreamable {
        public MySizeData() {}
        public MySizeData(int num, byte[] buf) {super(num, buf);}
        public int serializedSize() {
            return Global.INT_SIZE*2 + (array != null? array.length : 0);
        }
    }

    protected static class Person implements Serializable {
        private static final long serialVersionUID=8635045223414419580L;
        protected String name;
        protected int    age;
        protected byte[] buf;

        public Person(String name, int age, byte[] buf) {
            this.name=name;
            this.age=age;
            this.buf=buf;
        }

        public String toString() {
            return String.format("name=%s age=%d bytes=%d", name, age, buf != null? buf.length : 0);
        }
    }



}


