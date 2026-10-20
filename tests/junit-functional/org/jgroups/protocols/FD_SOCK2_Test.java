package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.ThreadPool;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.testng.Assert.expectThrows;

/**
 * Test for {@link FD_SOCK2} not closing its sockets within the {@link FD_SOCK2#stop()} call and subsequently failing
 * when subsequently started again (e.g. triggered by redeploy in WildFly). Reproducible by connecting and disconnecting
 * the channel in a loop. Typically happens on a modern machine within seconds.
 *
 * @author Radoslav Husar
 * @see <a href="https://issues.redhat.com/browse/JGRP-2647">JGRP-2647</a>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class FD_SOCK2_Test {

    protected JChannel a, b, c;

    public void testSocketClosing() throws Exception {
        a = new JChannel(Util.getTestStack(
                new FD_SOCK2().setPortRange(1).setOffset(1))
        ).name("A");

        for (int i = 0; i < 1_000; i++) {
            try {
                a.connect(FD_SOCK2_Test.class.getSimpleName());
            } catch (Exception e) {
                Logger.getLogger(FD_SOCK2_Test.class.getSimpleName()).info(String.format("FD_SOCK2 socket close test failed on run #%d", i));
                throw e;
            }

            a.disconnect();
        }
    }

    @DataProvider
    protected Object[][] clusterSizes() {
        return new Object[][]{{2}, {3}};
    }

    @Test(dataProvider="clusterSizes")
    public void testRestartWhileConnected(int size) throws Exception {
        createChannels(size);
        FD_SOCK2 fd=fd(a);
        Address address=a.getAddress();
        View view=a.getView();
        for(int i=0; i < 5; i++) {
            fd.stop();
            assert a.isConnected();
            assert !fd.srv.running();
            assert fd.ping_dest.dest() == null;
            fd.start();
            awaitMonitoring(a);
            assert a.getAddress().equals(address);
            assert a.getView().equals(view);
        }
    }

    public void testViewChangesWhileStopped() throws Exception {
        a=createChannel("A");
        FD_SOCK2 fd=fd(a);
        fd.stop();
        b=createChannel("B");
        Util.waitUntilAllChannelsHaveSameView(5000, 20, a, b);
        assert fd.members.getMembers().equals(a.getView().getMembers());
        assert fd.pingable_mbrs.isEmpty();
        assert fd.ping_dest.dest() == null;
        assert !fd.srv.running();
        assert fd.num_suspect_events == 0;

        fd.start();
        awaitMonitoring(a);
        assert fd.ping_dest.dest().equals(b.getAddress());
    }

    public void testDelayedConnectionClosedAfterRestart() throws Exception {
        createChannels(2);
        FD_SOCK2 fd=fd(a);
        List<Runnable> pending=new ArrayList<>();
        fd.thread_pool=new ThreadPool() {
            public boolean execute(Runnable task) {
                pending.add(task);
                return true;
            }
        };
        Connection conn=fd.srv.getConnection(fd.ping_dest.destPhysical());
        fd.connectionClosed(conn);
        assert pending.size() == 1;
        fd.stop();
        fd.start();
        awaitMonitoring(a);
        int suspects=fd.num_suspect_events;

        pending.get(0).run();
        assert fd.num_suspect_events == suspects;
        assert fd.pingable_mbrs.contains(b.getAddress());
        assert fd.ping_dest.connected();
    }

    public void testStopDuringConnect() throws Exception {
        createChannels(2);
        FD_SOCK2 fd=fd(a), peer=fd(b);
        Receiver receiver=peer.srv.receiver();
        CountDownLatch received=new CountDownLatch(1);
        peer.srv.receiver(new Receiver() {
            public void receive(Address sender, byte[] buf, int offset, int length) {
                received.countDown(); // withhold CONNECT-RSP so stop() must wake the connection wait
            }

            public void receive(Address sender, java.io.DataInput in, int length) {
                received.countDown();
            }
        });
        var executor=Executors.newFixedThreadPool(2);
        try {
            fd.connect_timeout=30000;
            fd.ping_dest.reset();
            var connect=executor.submit(() -> fd.handleView(a.getView()));
            assert received.await(5, TimeUnit.SECONDS);
            var stop=executor.submit(fd::stop);
            stop.get(5, TimeUnit.SECONDS);
            connect.get(5, TimeUnit.SECONDS);
            assert !fd.srv.running();
            assert fd.ping_dest.dest() == null;
            assert fd.num_suspect_events == 0;
        }
        finally {
            peer.srv.receiver(receiver);
            fd.ping_dest.setConnectResult(false);
            executor.shutdownNow();
            assert executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public void testUnsuspectRestoresDisconnectedMonitoring() throws Exception {
        createChannels(2);
        FD_SOCK2 fd=fd(a);
        fd.ping_dest.clientState(FD_SOCK2.State.DISCONNECTED);
        fd.handle(new FD_SOCK2.FdHeader(FD_SOCK2.FdHeader.UNSUSPECT).mbrs(List.of(b.getAddress())), b.getAddress());
        awaitMonitoring(a);
    }

    public void testFailedRestartCanBeRetried() throws Exception {
        FailingStartFD fd=new FailingStartFD();
        a=createChannel("A", fd);
        b=createChannel("B");
        Util.waitUntilAllChannelsHaveSameView(5000, 20, a, b);
        awaitMonitoring(a);
        fd.stop();
        fd.fail_start=true;
        expectThrows(IllegalStateException.class, fd::start);
        assert !fd.running;
        assert !fd.srv.running();
        fd.fail_start=false;
        fd.start();
        awaitMonitoring(a);
    }

    public void testReconnectDoesNotRestorePreviousMembership() throws Exception {
        createChannels(2);
        FD_SOCK2 fd=fd(a);
        a.disconnect();
        assert fd.members.isEmpty();
        a.connect(FD_SOCK2_Test.class.getSimpleName() + "-other");
        assert a.getView().size() == 1;
        assert fd.members.getMembers().equals(List.of(a.getAddress()));
        assert fd.ping_dest.dest() == null;
        assert fd.num_suspect_events == 0;
    }

    @Test(dataProvider="clusterSizes")
    public void testQuarantineAndMerge(int size) throws Exception {
        createChannels(size);
        List<JChannel> channels=size == 2? List.of(a, b) : List.of(a, b, c);
        for(JChannel ch: channels) {
            VERIFY_SUSPECT2 verify=ch.getProtocolStack().findProtocol(VERIFY_SUSPECT2.class);
            verify.stop();
            verify.setTimeout(200);
            verify.start();
        }
        Address address=a.getAddress();
        FD_SOCK2 fd=fd(a);
        DISCARD discard=a.getProtocolStack().findProtocol(DISCARD.class);
        discard.discardAll(true);
        fd.stop();
        Util.waitUntil(5000, 20, () -> b.getView().size() == size-1 && !b.getView().containsMember(address));
        if(c != null)
            Util.waitUntilAllChannelsHaveSameView(5000, 20, b, c);

        // Other detectors provide this local singleton transition in a quarantine stack.
        List<Address> peers=new ArrayList<>(a.getView().getMembers());
        peers.remove(address);
        a.getProtocolStack().findProtocol(GMS.class).up(new Event(Event.SUSPECT, peers));
        Util.waitUntil(5000, 20, () -> a.getView().size() == 1);
        fd.start();
        assert fd.ping_dest.dest() == null;
        discard.discardAll(false);
        Util.waitUntil(10000, 100,
                      () -> channels.stream().allMatch(ch -> ch.getView().size() == size
                        && ch.getView().equals(a.getView())),
                      () -> Util.printViews(channels.toArray(JChannel[]::new)),
                      () -> {
                          for(JChannel ch: channels) {
                              MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
                              merge.sendInfo();
                          }
                          for(JChannel ch: channels) {
                              MERGE3 merge=ch.getProtocolStack().findProtocol(MERGE3.class);
                              merge.checkInconsistencies();
                          }
                      });
        for(JChannel ch: channels)
            awaitMonitoring(ch);
        assert a.getAddress().equals(address);
    }

    protected void createChannels(int size) throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        if(size == 3)
            c=createChannel("C");
        JChannel[] channels=size == 2? new JChannel[]{a, b} : new JChannel[]{a, b, c};
        Util.waitUntilAllChannelsHaveSameView(5000, 20, channels);
        for(JChannel ch: channels)
            awaitMonitoring(ch);
    }

    protected static JChannel createChannel(String name) throws Exception {
        return createChannel(name, new FD_SOCK2());
    }

    protected static JChannel createChannel(String name, FD_SOCK2 fd) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(), new SHARED_LOOPBACK_PING(),
                            new DISCARD().excludeItself(true),
                            new MERGE3().setMinInterval(10000).setMaxInterval(20000).setCheckInterval(30000),
                            fd, new VERIFY_SUSPECT2().setTimeout(10000),
                            new NAKACK2(), new UNICAST3(), new STABLE(),
                            new GMS().printLocalAddress(false).setJoinTimeout(100).setLeaveTimeout(100)
                              .setViewAckCollectionTimeout(100).setMergeTimeout(2000))
          .name(name).connect(FD_SOCK2_Test.class.getSimpleName());
    }

    protected static FD_SOCK2 fd(JChannel ch) {
        return ch.getProtocolStack().findProtocol(FD_SOCK2.class);
    }

    protected static void awaitMonitoring(JChannel ch) throws Exception {
        FD_SOCK2 fd=fd(ch);
        Util.waitUntil(5000, 20, () -> {
            fd.lock.lock();
            try {
                return fd.ping_dest.connected()
                  && ch.getView().getMembers().contains(fd.ping_dest.dest())
                  && !ch.getAddress().equals(fd.ping_dest.dest());
            }
            finally {
                fd.lock.unlock();
            }
        }, fd::getPingDest);
    }

    protected static class FailingStartFD extends FD_SOCK2 {
        protected boolean fail_start;

        protected FailingStartFD() {
            id=ClassConfigurator.getProtocolId(FD_SOCK2.class);
        }

        protected NioServer createServer(int[] bind_ports) {
            if(fail_start)
                throw new IllegalStateException("Injected bind failure");
            return super.createServer(bind_ports);
        }
    }

    @AfterMethod
    protected void destroy() {
        Util.close(c, b, a);
        a=b=c=null;
    }
}
