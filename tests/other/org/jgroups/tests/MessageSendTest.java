package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FRAG;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.FRAG3;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckCollector;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Tests sending different types of {@link Message} implementations over a given protocol stack
 * @author Bela Ban
 * @since  5.0
 */
public class MessageSendTest implements Receiver {
    protected JChannel           ch;
    protected final AckCollector acks=new AckCollector();
    protected View               view;
    protected static short       ID=5678;


    static {
        ClassConfigurator.add(ID, MyHeader.class);
    }


    protected void start(String props, String name) throws Exception {
        ch=new JChannel(props).name(name).setReceiver(this).connect("demo");

        int len=findFragSize(ch) *2;

        int i=1;
        while(Util.keyPress(": ") != 'x') {

            Message msg=new EmptyMessage();
            send(msg);

            msg=new BytesMessage(null, new byte[len]);
            send(msg);

            MyData obj=new MyData(i++, len);
            msg=new BytesMessage(null, obj);
            send(msg);

            msg=new ObjectMessage(null, obj);
            send(msg);

            obj=new MySizeData(i++, len);
            msg=new ObjectMessage(null, obj);
            send(msg);

            msg=new ObjectMessage(null, obj);
            send(msg);

            Person p=new Person("Bela Ban", 53, new byte[len]);
            msg=new ObjectMessage(null, p);
            send(msg);

            ByteBuffer b=ByteBuffer.wrap(new byte[len]);
            msg=new NioMessage(null, b);
            send(msg);

            byte[] tmp=new byte[len];
            b=ByteBuffer.allocateDirect(len).put(tmp);
            b.flip();
            msg=new NioMessage(null, b);
            send(msg);
        }
        Util.close(ch);
    }


    protected void send(Message msg) throws Exception {
        acks.reset(view.getMembers());
        System.out.printf("-- sending %s of %d bytes: ", msg.getClass().getSimpleName(), msg.getLength());
        ch.send(msg);
        boolean success;

        try {
            success=acks.waitForAllAcks(5000);
        }
        catch(TimeoutException ex) {
            success=false;
        }
        System.out.printf("%s\n", success? "OK" : "FAIL");
    }

    protected static int findFragSize(JChannel ch) {
        Protocol prot=ch.getProtocolStack().findProtocol(FRAG.class, FRAG2.class, FRAG3.class);
        return prot != null? (int)prot.getValue("frag_size") : 30_000;
    }

    public void receive(Message msg) {
        if(msg.getHeader((short)3344) != null) {
            acks.ack(msg.getSrc());
            return;
        }

        if(!msg.getSrc().equals(ch.getAddress()))
            System.out.printf("-- received %d bytes from %s (msg type: %s)\n",
                              msg.getLength(), msg.getSrc(), msg.getClass().getSimpleName());
        try {
            ch.send(new EmptyMessage(msg.getSrc()).putHeader((short)3344, new MyHeader()));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }


    public void viewAccepted(View view) {
        System.out.printf("** view: %s\n", (this.view=view));
    }

    public static void main(String[] args) throws Exception {
        String props=null, name=null;
        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            System.out.printf("%s [-props config] [-name name]\n", MessageSendTest.class.getSimpleName());
            return;
        }
        new MessageSendTest().start(props, name);
    }


    protected static class MyHeader extends Header {
        public MyHeader() {}
        public short getMagicId()                            {return ID;}
        public Supplier<? extends Header> create()           {return MyHeader::new;}
        public int  serializedSize()                         {return 0;}
        public void writeTo(DataOutput out) throws IOException {}
        public void readFrom(DataInput in)  throws IOException {}
    }

    public static class MyData implements Streamable {
        protected int    num;
        protected byte[] data;

        public MyData() {
        }

        public MyData(int num, int length) {
            this.num=num;
            this.data=new byte[length];
        }

        public MyData(int num, byte[] buf) {
            this.num=num;
            this.data=buf;
        }

        public String toString() {
            return String.format("num=%d, data: %d bytes", num, data != null? data.length : 0);
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(num);
            out.writeInt(data != null? data.length : 0);
            if(data != null)
                out.write(data, 0, data.length);
        }

        public void readFrom(DataInput in) throws IOException {
            num=in.readInt();
            int len=in.readInt();
            if(len > 0) {
                data=new byte[len];
                in.readFully(data);
            }
        }
    }


    public static class MySizeData extends MyData implements SizeStreamable {

        public MySizeData() {
        }

        public MySizeData(int num, int length) {
            super(num, length);
        }

        public MySizeData(int num, byte[] buf) {
            super(num, buf);
        }

        public int serializedSize() {
            return Global.INT_SIZE*2 + (data != null? data.length : 0);
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



