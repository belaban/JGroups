package org.jgroups.protocols;



import org.testng.annotations.*;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.debug.Simulator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Vector;

/**
 * Tests the fragmentation (FRAG) protocol for http://jira.jboss.com/jira/browse/JGRP-215
 * @author Bela Ban
 */
@Test(groups={"temp", "protocols"})
public class FRAG_Test extends ChannelTestBase {
    private IpAddress a1;
    private Vector members;
    private View v;
    private Simulator s=null;
    private int num_done=0;

    private Sender[] senders=null;

    public static final int SIZE=10000; // bytes
    public static final int NUM_MSGS=10;
    public static final int NUM_THREADS=100;


    @BeforeMethod
    void setUp() throws Exception {
        a1=new IpAddress(1111);
        members=new Vector();
        members.add(a1);
        v=new View(a1, 1, members);
        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);
        Protocol frag=createProtocol();
        System.out.println("protocol to be tested: " + frag.getName());
        Properties props=new Properties();
        props.setProperty("frag_size", "512");
        frag.setPropertiesInternal(props);
        Protocol[] stack=new Protocol[]{frag};
        s.setProtocolStack(stack);
        s.start();
    }


    protected Protocol createProtocol() {
        return new FRAG();
    }

    @AfterMethod
    void tearDown() throws Exception {
        s.stop();
    }



    public void testFragmentation() throws InterruptedException {
        FRAG_Test.Receiver r=new FRAG_Test.Receiver();
        s.setReceiver(r);

        senders=new Sender[NUM_THREADS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(i);
        }

        for(int i=0; i < senders.length; i++) {
            Sender sender=senders[i];
            sender.start();
        }

        for(int i=0; i < senders.length; i++) {
            Sender sender=senders[i];
            sender.join(5000);
            if(sender.isAlive()) {
                System.err.println("sender #" + i + " could not be joined (still alive)");
            }
        }

        int sent=0, received=0, corrupted=0;
        for(int i=0; i < senders.length; i++) {
            Sender sender=senders[i];
            received+=sender.getNumReceived();
            sent+=sender.getNumSent();
            corrupted+=sender.getNumCorrupted();
        }

        System.out.println("sent: " + sent + ", received: " + received + ", corrupted: " + corrupted);
        assert sent == received : "sent and received should be the same (sent=" + sent + ", received=" + received + ")";
        assert corrupted == 0 : "we should have 0 corrupted messages";
    }




    class Sender extends Thread {
        int id=-1;
        int num_sent=0;
        int num_received=0;
        int num_corrupted=0;
        boolean done=false;

        public int getIdent() {
            return id;
        }

        public int getNumReceived() {
            return num_received;
        }

        public int getNumSent() {
            return num_sent;
        }

        public int getNumCorrupted() {
            return num_corrupted;
        }

        public Sender(int id) {
            super("sender #" + id);
            this.id=id;
        }

        public void run() {
            byte[] buf=createBuffer(id);
            Message msg;
            Event evt;

            for(int i=0; i < NUM_MSGS; i++) {
                msg=new Message(null, null, buf);
                evt=new Event(Event.MSG, msg);
                s.send(evt);
                num_sent++;
            }

            synchronized(this) {
                try {
                    while(!done)
                        this.wait(500);
                    num_done++;
                    System.out.println("thread #" + id  + " is done (" + num_done + ")");
                }
                catch(InterruptedException e) {
                }
            }
        }

        private byte[] createBuffer(int id) {
            ByteBuffer buf=ByteBuffer.allocate(SIZE);
            int elements=SIZE / Global.INT_SIZE;
            for(int i=0; i < elements; i++) {
                buf.putInt(id);
            }
            return buf.array();
        }

        /** 1 int has already been read by the Receiver */
        public void verify(ByteBuffer buf) {
            boolean corrupted=false;

            int num_elements=(SIZE / Global.INT_SIZE) -1;
            int tmp;
            for(int i=0; i < num_elements; i++) {
                tmp=buf.getInt();
                if(tmp != id) {
                    corrupted=true;
                    break;
                }
            }

            if(corrupted)
                num_corrupted++;
            else
                num_received++;

            if(num_corrupted + num_received >= NUM_MSGS) {
                synchronized(this) {
                    done=true;
                    this.notify();
                }
            }
        }
    }

    class Receiver implements Simulator.Receiver {
        int received=0;

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                received++;
                if(received % 1000 == 0)
                    System.out.println("<== " + received);

                Message msg=(Message)evt.getArg();
                byte[] data=msg.getBuffer();
                ByteBuffer buf=ByteBuffer.wrap(data);
                int id=buf.getInt();
                Sender sender=senders[id];
                sender.verify(buf);
            }
        }
    }


}
