// $Id: ConnectionTableUnitTest.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.blocks.ConnectionTable;
import org.jgroups.Message;
import org.jgroups.Address;


/**
 */
public class ConnectionTableUnitTest extends TestCase {
    ConnectionTable ct1, ct2;
    final int       port1=5555, port2=6666;




    public ConnectionTableUnitTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        ct1=new ConnectionTable(port1);
        log("address of ct1: " + ct1.getLocalAddress());
        ct2=new ConnectionTable(port2);
        log("address of ct2: " + ct2.getLocalAddress());
    }

    public void tearDown() throws Exception {
        if(ct1 != null) {
            ct1.stop();
            ct1=null;
        }
        if(ct2 != null) {
            ct2.stop();
            ct2=null;
        }
    }

    public void testSetup() {
        assertNotSame(ct1.getLocalAddress(), ct2.getLocalAddress());
    }


    public void testSendToSelf() throws Exception {
        long       NUM=1000, total_time;
        Address    myself=ct1.getLocalAddress();
        MyReceiver r=new MyReceiver(ct1, NUM, false);

        ct1.setReceiver(r);

        for(int i=0; i < NUM; i++) {
            ct1.send(new Message(myself, null, null));
        }
        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived()  + " ms/msg)");

        assertEquals(r.getNumExpected(), r.getNumReceived());
    }

    public void testSendToOther() throws Exception {
        long       NUM=1000, total_time;
        Address    other=ct2.getLocalAddress();
        MyReceiver r=new MyReceiver(ct2, NUM, false);

        ct2.setReceiver(r);

        for(int i=0; i < NUM; i++) {
            ct1.send(new Message(other, null, null));
        }
        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived()  + " ms/msg)");

        assertEquals(r.getNumExpected(), r.getNumReceived());
    }


    public void testSendToOtherGetResponse() throws Exception {
        long       NUM=1000, total_time;
        Address    other=ct2.getLocalAddress();
        MyReceiver r1=new MyReceiver(ct1, NUM, false);
        MyReceiver r2=new MyReceiver(ct2, NUM, true); // send response

        ct1.setReceiver(r1);
        ct2.setReceiver(r2);

        for(int i=0; i < NUM; i++) {
            ct1.send(new Message(other, null, null));
        }
        log("sent " + NUM + " msgs");
        r1.waitForCompletion();
        total_time=r1.stop_time - r1.start_time;
        log("number expected=" + r1.getNumExpected() + ", number received=" + r1.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r1.getNumReceived()  + " ms/msg)");

        assertEquals(r1.getNumExpected(), r1.getNumReceived());
    }


    void log(String msg) {
        System.out.println("-- [" + Thread.currentThread() + "]: " + msg);
    }


    public static void main(String[] args) {
        String[] testCaseName={ConnectionTableUnitTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }



    class MyReceiver implements ConnectionTable.Receiver {
        long            num_expected=0, num_received=0, start_time=0, stop_time=0;
        boolean         done=false, send_response=false;
        long            modulo;
        ConnectionTable ct;

        MyReceiver(ConnectionTable ct, long num_expected, boolean send_response) {
            this.ct=ct;
            this.num_expected=num_expected;
            this.send_response=send_response;
            start_time=System.currentTimeMillis();
            modulo=num_expected / 10;
        }


        public long getNumReceived() {
            return num_received;
        }

        public long getNumExpected() {
            return num_expected;
        }

        public void receive(Message msg) {
            Address sender=msg.getSrc();
            num_received++;
            if(num_received % modulo == 0)
                log("received msg# " + num_received);
            if(send_response) {
                if(ct != null)
                    ct.send(new Message(sender, null, null));
            }
            if(num_received >= num_expected) {
                synchronized(this) {
                    if(!done) {
                        done=true;
                        stop_time=System.currentTimeMillis();
                        notifyAll();
                    }
                }
            }
        }

        public void waitForCompletion() {
            synchronized(this) {
                while(!done) {
                    try {
                        wait();
                    }
                    catch(InterruptedException e) {
                    }
                }
            }
        }


    }

}
