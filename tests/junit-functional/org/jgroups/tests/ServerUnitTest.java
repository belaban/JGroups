
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.util.Bits;
import org.jgroups.util.CondVar;
import org.jgroups.util.Condition;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ServerUnitTest {
    protected static final InetAddress bind_addr;

    static {
        try {
            bind_addr=Util.getLoopback();
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

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
                send(data, a, myself);
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
                    send(data, a, myself);
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
                // a.send(b.localAddress(), new byte[1000], 0, 1000);
                send(data, a, b.localAddress());
                Util.sleep(1000);

                a.receiver(r1);
                b.receiver(r2);
                for(int i=0; i < NUM; i++)
                    send(data, a, null);

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
                    send(data, a, other);

                log("sent " + NUM + " msgs");
                r.waitForCompletion(20000);
                total_time=r.stop_time - r.start_time;
                log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
                      ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived() + " ms/msg)");
                Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
            }
        }
    }


    // @Test(invocationCount=100)
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
                    send(data, a, other);
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

                send(data, a, addr2);
                send(data, b, addr1);

                String msg="A: " + a + "\nB: " + b;
                System.out.println(msg);

                waitForOpenConns(1, a, b);

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
                send(data, a, addr1); // send to self
                assert a.getNumConnections() == 0;
                send(data, a, addr2);  // send to other

                send(data, b, addr2); // send to self
                send(data, b, addr1); // send to other


                System.out.println("A:\n" + a + "\nB:\n" + b);

                int num_conns_table1=a.getNumConnections(), num_conns_table2=b.getNumConnections();
                assert num_conns_table1 == 1 : "A should have 1 connection, but has " + num_conns_table1 + ": " + a;
                assert num_conns_table2 == 1 : "B should have 1 connection, but has " + num_conns_table2 + ": " + b;

                Util.close(b,a);
                waitForOpenConns(0, a, b);
            }
        }
    }


    public void testAsyncConnectThenSend() throws Exception {
        try(NioServer a=(NioServer)create(true, 0); NioServer b=(NioServer)create(true, 0)) {
            a.start();
            b.start();
            Address target=b.localAddress();
            MyReceiver r=new MyReceiver(b, 2, false);
            b.receiver(r);

            // now send 2 msgs from A to B: this will connect async, buffer the 2 msgs, then send when connected
            byte[] buffer="hello world".getBytes();
            send(buffer, a, target);
            send(buffer, a, target);
            r.waitForCompletion(20000);
            assert r.getNumReceived() == 2;
        }
    }

    protected static void send(byte[] request, BaseServer server, Address dest) {
        byte[] data=new byte[request.length + Global.INT_SIZE];
        Bits.writeInt(request.length, data, 0);
        System.arraycopy(request, 0, data, Global.INT_SIZE, request.length);
        try {
            server.send(dest, data, 0, data.length);
        }
        catch(Exception e) {
            System.err.println("Failed sending a request to " + dest + ": " + e);
        }
    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread() + "]: " + msg);
    }

    protected static BaseServer create(boolean nio, int port) {
        try {
            BaseServer retval=nio? new NioServer(bind_addr, port).maxSendBuffers(1024)
              : new TcpServer(bind_addr, port);
            retval.usePeerConnections(true);
            retval.start();
            return retval;
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected static void waitForOpenConns(int expected, BaseServer... servers) throws Exception {
        for(int i=0; i < 20; i++) {
            if(!Arrays.stream(servers).allMatch(srv -> srv.getNumOpenConnections() == expected))
                Util.sleep(1000);
        }
        if(!Arrays.stream(servers).allMatch(srv -> srv.getNumOpenConnections() == expected)) {
            String msg=String.format("expected connections: %d, actual:\n%s\n", expected,
                                     Stream.of(servers).map(s -> String.format("%s: %s", s.getNumOpenConnections(), s.printConnections()))
                                       .collect(Collectors.joining("\n")));
            throw new Exception(msg);
        }
    }


    protected static void connectionEstablished(BaseServer server, Address dest) {
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
        protected final Condition  cond;

        MyReceiver(BaseServer server, long num_expected, boolean send_response) {
            this.server=server;
            this.num_expected=num_expected;
            this.send_response=send_response;
            start_time=System.currentTimeMillis();
            modulo=num_expected / 10;
            cond=() -> num_received.get() >= num_expected;
        }


        public synchronized long getNumReceived() {return num_received.get();}
        public synchronized long getNumExpected() {return num_expected;}


        public synchronized void receive(Address sender, byte[] data, int offset, int length) {
            // System.out.printf("[nio] from %s: %d bytes\n", sender, length);
            long tmp=num_received.incrementAndGet();
            if(tmp >= num_expected) {
                if(stop_time == 0)
                    stop_time=System.currentTimeMillis();
                done.signal(true);
            }
            if(send_response && tmp <= num_expected) {
                try {
                    byte[] rsp=new byte[0];
                    send(rsp, server, sender);
                    num_sent.incrementAndGet();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public synchronized void receive(Address sender, DataInput in) throws Exception {
            int len=in.readInt();
            byte[] buf=new byte[len];
            in.readFully(buf);
            // System.out.printf("[tcp] from %s: %d bytes\n", sender, len);
            long tmp=num_received.incrementAndGet();
            if(tmp >= num_expected) {
                if(stop_time == 0)
                    stop_time=System.currentTimeMillis();
                done.signal(true);
            }
            if(send_response && tmp <= num_expected) {
                try {
                    byte[] rsp=new byte[0];
                    send(rsp, server, sender);
                    num_sent.incrementAndGet();
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void waitForCompletion(long timeout) throws Exception {
            done.waitFor(cond, timeout, TimeUnit.MILLISECONDS);
        }

        public String toString() {
            return String.format("expected=%d, received=%d\n", num_expected, num_received.get());
        }
    }


}
