
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.ConnectionTable;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * @author Bela Ban
 * @version $Id: ConnectionTableUnitTest.java,v 1.4 2008/04/08 12:25:28 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ConnectionTableUnitTest {
    ConnectionTable  ct1, ct2;
    static final int port1=5555, port2=6666;




    @BeforeMethod
    protected void setUp() throws Exception {
        ct1=new ConnectionTable(port1);
        ct1.setUseSendQueues(false);
        ct1.start();
        log("address of ct1: " + ct1.getLocalAddress());
        ct2=new ConnectionTable(port2);
        ct2.setUseSendQueues(false);
        ct2.start();
        log("address of ct2: " + ct2.getLocalAddress());
    }

    @AfterMethod
    void tearDown() throws Exception {
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
        Assert.assertNotSame(ct1.getLocalAddress(), ct2.getLocalAddress());
    }

    public void testSendToNullReceiver() throws Exception {
        byte[]  data=new byte[0];
        ct1.send(null, data, 0, data.length);
    }

    public void testSendEmptyData() throws Exception {
        byte[]  data=new byte[0];
        Address myself=ct1.getLocalAddress();
        ct1.send(myself, data, 0, data.length);
    }

    public void testSendNullData() throws Exception {
        Address myself=ct1.getLocalAddress();
        ct1.send(myself, null, 0, 0);
    }


    public void testSendToSelf() throws Exception {
        long       NUM=1000, total_time;
        Address    myself=ct1.getLocalAddress();
        MyReceiver r=new MyReceiver(ct1, NUM, false);
        byte[]     data=new byte[] {'b', 'e', 'l', 'a'};

        ct1.setReceiver(r);

        for(int i=0; i < NUM; i++) {
            ct1.send(myself, data, 0, 0);
        }
        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived()  + " ms/msg)");

        Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
    }

    public void testSendToOther() throws Exception {
        long       NUM=1000, total_time;
        Address    other=ct2.getLocalAddress();
        MyReceiver r=new MyReceiver(ct2, NUM, false);
        byte[]     data=new byte[] {'b', 'e', 'l', 'a'};

        ct2.setReceiver(r);

        for(int i=0; i < NUM; i++) {
            ct1.send(other, data, 0, 0);
        }
        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived()  + " ms/msg)");

        Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
    }


    public void testSendToOtherGetResponse() throws Exception {
        long       NUM=1000, total_time;
        Address    other=ct2.getLocalAddress();
        MyReceiver r1=new MyReceiver(ct1, NUM, false);
        MyReceiver r2=new MyReceiver(ct2, NUM, true); // send response
        byte[]     data=new byte[] {'b', 'e', 'l', 'a'};

        ct1.setReceiver(r1);
        ct2.setReceiver(r2);

        for(int i=0; i < NUM; i++) {
            ct1.send(other, data, 0, 0);
        }
        log("sent " + NUM + " msgs");
        r1.waitForCompletion();
        total_time=r1.stop_time - r1.start_time;
        log("number expected=" + r1.getNumExpected() + ", number received=" + r1.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r1.getNumReceived()  + " ms/msg)");

        Assert.assertEquals(r1.getNumExpected(), r1.getNumReceived());
    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread() + "]: " + msg);
    }





    static class MyReceiver implements ConnectionTable.Receiver {
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


        public void receive(Address sender, byte[] data, int offset, int length) {
            num_received++;
            if(num_received % modulo == 0)
                log("received msg# " + num_received);
            if(send_response) {
                if(ct != null) {
                    try {
                        byte[] rsp=new byte[0];
                        ct.send(sender, rsp, 0, 0);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
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
