
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.util.CondVar;
import org.jgroups.util.Condition;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ServerUnitTest {

    public void testSetup() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {
                Assert.assertNotSame(a.localAddress(), b.localAddress());
            }
        }
    }


    public void testSendEmptyData() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0)) {
                byte[] data=new byte[0];
                Address myself=a.localAddress();
                a.receiver(new ReceiverAdapter() {});
                a.send(myself, data, 0, data.length);
            }
        }
    }

    public void testSendNullData() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0)) {
                Address myself=a.localAddress();
                a.send(myself, null, 0, 0); // the test passes if send() doesn't throw an exception
            }
        }
    }


    public void testSendToSelf() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0)) {
                long NUM=1000, total_time;
                Address myself=a.localAddress();
                MyReceiver r=new MyReceiver(a, NUM, false);
                byte[] data="hello world".getBytes();
                a.receiver(r);
                for(int i=0; i < NUM; i++)
                    a.send(myself, data, 0, data.length);
                log("sent " + NUM + " msgs");
                r.waitForCompletion(20000);
                total_time=r.stop_time - r.start_time;
                log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
                      ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived() + " ms/msg)");

                Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
            }
        }
    }

    public void testSendToAll() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {
                long NUM=1000, total_time;
                MyReceiver r1=new MyReceiver(a, NUM, false);
                MyReceiver r2=new MyReceiver(b, NUM, false);
                byte[] data="hello world".getBytes();

                // send uncast to establish connection to s2:
                a.send(b.localAddress(), new byte[1000], 0, 1000);
                Util.sleep(1000);

                a.receiver(r1);
                b.receiver(r2);
                for(int i=0; i < NUM; i++)
                    a.send(null, data, 0, data.length);

                log("sent " + NUM + " msgs");
                r2.waitForCompletion(20000);
                total_time=r2.stop_time - r2.start_time;
                log("number expected=" + r2.getNumExpected() + ", number received=" + r2.getNumReceived() +
                      ", total time=" + total_time + " (" + (double)total_time / r2.getNumReceived() + " ms/msg)");

                Assert.assertEquals(r2.getNumExpected(), r2.getNumReceived());
                assert r1.getNumReceived() == 0 || r1.getNumReceived() > 0;
            }
        }
    }

    public void testSendToOther() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {
                long NUM=1000, total_time;
                Address other=b.localAddress();
                MyReceiver r=new MyReceiver(b, NUM, false);
                byte[] data="hello world".getBytes();

                b.receiver(r);
                for(int i=0; i < NUM; i++)
                    a.send(other, data, 0, data.length);

                log("sent " + NUM + " msgs");
                r.waitForCompletion(20000);
                total_time=r.stop_time - r.start_time;
                log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
                      ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived() + " ms/msg)");
                Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
            }
        }
    }



    public void testSendToOtherGetResponse() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {
                long NUM=1000, total_time;
                Address other=b.localAddress();
                MyReceiver r1=new MyReceiver(a, NUM, false);
                MyReceiver r2=new MyReceiver(b, NUM, true); // send response
                byte[] data="hello world".getBytes();

                a.receiver(r1);
                b.receiver(r2);

                for(int i=0; i < NUM; i++)
                    a.send(other, data, 0, data.length);
                log("sent " + NUM + " msgs");
                r1.waitForCompletion(20000);
                total_time=r1.stop_time - r1.start_time;
                log(String.format("r1.expected=%d, r1.received=%d, r2.expected=%d, r2.received=%d, r2.sent=%d, total time=%d (%.2f ms/msg)",
                                  r1.getNumExpected(), r1.getNumReceived(), r1.getNumExpected(), r2.getNumReceived(), r2.num_sent.get(),
                                  total_time, (double)total_time / r1.getNumReceived()
                ));

                Assert.assertEquals(r1.getNumReceived(), r1.getNumExpected());
            }
        }
    }


    /**
     * A connects to B and B connects to A at the same time. This test makes sure we only have <em>one</em> connection,
     * not two, e.g. a spurious connection. Tests http://jira.jboss.com/jira/browse/JGRP-549.<p/>
     * Turned concurrent test into a simple sequential test. We're going to replace this code with NIO2 soon anyway...
     */
    public void testReuseOfConnection() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {

                int num_conns;
                num_conns=a.getNumConnections();
                assert num_conns == 0;
                num_conns=b.getNumConnections();
                assert num_conns == 0;

                Address addr1=a.localAddress(), addr2=b.localAddress();
                byte[] data="hello world".getBytes();

                a.send(addr2, data, 0, data.length);
                b.send(addr1, data, 0, data.length);

                String msg="A: " + a + "\nB: " + b;
                System.out.println(msg);

                waitForOpenConns(1, a, b);
                num_conns=a.getNumOpenConnections();
                assert num_conns == 1 : "num_conns for A is " + num_conns + ", " + msg;
                num_conns=b.getNumOpenConnections();
                assert num_conns == 1 : "num_conns for B is " + num_conns + ", " + msg;

                // done in a loop because connect() might be non-blocking (NioServer)
                connectionEstablished(a, addr2);
                connectionEstablished(b, addr1);
            }
        }
    }


    public void testConnectionCountOnStop() throws Exception {
        for(boolean nio : new boolean[]{false, true}) {
            try(BaseServer a=create(nio, 0);
                BaseServer b=create(nio, 0)) {
                Address addr1=a.localAddress(), addr2=b.localAddress();
                byte[] data="hello world".getBytes();
                a.send(addr1, data, 0, data.length); // send to self
                assert a.getNumConnections() == 0;
                a.send(addr2, data, 0, data.length); // send to other

                b.send(addr2, data, 0, data.length); // send to self
                b.send(addr1, data, 0, data.length); // send to other


                System.out.println("A:\n" + a + "\nB:\n" + b);

                int num_conns_table1=a.getNumConnections(), num_conns_table2=b.getNumConnections();
                assert num_conns_table1 == 1 : "A should have 1 connection, but has " + num_conns_table1 + ": " + a;
                assert num_conns_table2 == 1 : "B should have 1 connection, but has " + num_conns_table2 + ": " + b;

                Util.close(b,a);
                waitForOpenConns(0, a, b);
                assert a.getNumOpenConnections() == 0 : "A should have 0 connections: " + a.printConnections();
                assert b.getNumOpenConnections() == 0 : "B should have 0 connections: " + b.printConnections();
            }
        }
    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread() + "]: " + msg);
    }

    protected static BaseServer create(boolean nio, int port) {
        try {
            BaseServer retval=nio? new NioServer(null, port).maxSendBuffers(1024).maxReadBatchSize(20)
              : new TcpServer(null, port).useSendQueues(false);
            retval.usePeerConnections(true);
            retval.start();
            // System.out.printf("Created instance of %s\n", retval.getClass().getSimpleName());
            return retval;
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected void waitForOpenConns(int expected, BaseServer... servers) {
        for(int i=0; i < 10; i++) {
            boolean all_ok=true;
            for(BaseServer server: servers) {
                if(server.getNumOpenConnections() != expected) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                return;
            Util.sleep(500);
        }
    }


    protected void connectionEstablished(BaseServer server, Address dest) {
        for(int i=0; i < 10; i++) {
            if(server.connectionEstablishedTo(dest))
                break;
            Util.sleep(500);
        }
        assert server.connectionEstablishedTo(dest);
    }




    protected static class MyReceiver extends ReceiverAdapter {
        protected final long       num_expected;
        protected final AtomicLong num_received=new AtomicLong(0), num_sent=new AtomicLong(0);
        protected long             start_time=0, stop_time=0;
        protected final  CondVar   done=new CondVar();
        protected boolean          send_response=false;
        protected final long       modulo;
        protected final BaseServer server;

        MyReceiver(BaseServer server, long num_expected, boolean send_response) {
            this.server=server;
            this.num_expected=num_expected;
            this.send_response=send_response;
            start_time=System.currentTimeMillis();
            modulo=num_expected / 10;
        }


        public long getNumReceived() {return num_received.get();}
        public long getNumExpected() {return num_expected;}


        public void receive(Address sender, byte[] data, int offset, int length) {
            long tmp=num_received.incrementAndGet();
            if(tmp >= num_expected) {
                synchronized(this) {
                    if(stop_time == 0)
                        stop_time=System.currentTimeMillis();
                }
                done.signal(true);
            }
            if(send_response && tmp <= num_expected) {
                try {
                    byte[] rsp=new byte[0];
                    server.send(sender, rsp, 0, rsp.length);
                    num_sent.incrementAndGet();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void waitForCompletion(long timeout) throws Exception {
            done.waitFor(new Condition() {
                public boolean isMet() {
                    return num_received.get() >= num_expected;
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }

        public String toString() {
            return String.format("expected=%d, received=%d\n", num_expected, num_received.get());
        }
    }


}
