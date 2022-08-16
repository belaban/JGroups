package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.RouterStub;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Tests interaction between {@link org.jgroups.stack.RouterStub} and {@link org.jgroups.stack.GossipRouter}
 * @author Bela Ban
 * @since  5.2.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RouterStubTest {
    protected GossipRouter             router_a, router_b;
    protected RouterStub               stub_one;
    protected final Address            stub_one_addr=Util.createRandomAddress("stub-one");
    protected final SocketFactory      socket_factory=new DefaultSocketFactory();
    protected RouterStubManager        stub_mgr;
    protected static final String      GROUP=RouterStubTest.class.getSimpleName();
    protected static final InetAddress BIND_ADDR;
    protected static final Log         LOG=LogFactory.getLog(RouterStubTest.class);
    protected TimeScheduler            timer=new TimeScheduler3();


    static {
        try {
            BIND_ADDR=Util.getLoopback();
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    protected void setUp() throws Exception {
        List<Integer> gossip_router_ports=ResourceManager.getNextTcpPorts(BIND_ADDR, 2);
        router_a=new GossipRouter(BIND_ADDR, gossip_router_ports.get(0));
        router_b=new GossipRouter(BIND_ADDR, gossip_router_ports.get(1));
    }

    @BeforeMethod protected void start() throws Exception {
        router_a.start();
        router_b.start();
    }

    @AfterMethod protected void stop() throws Exception {
        if(stub_one != null) {
            stub_one.disconnect(GROUP, stub_one_addr);
            stub_one.destroy();
        }
        if(stub_mgr != null) {
            stub_mgr.disconnectStubs();
            stub_mgr.destroyStubs();
        }
        router_a.stop();
        router_b.stop();
    }

    public void testConnect() throws Exception {
        stub_one=new RouterStub(null, new InetSocketAddress(BIND_ADDR, router_a.port()), false, null, socket_factory);
        stub_one.connect(GROUP, stub_one_addr, "stub-one", null);
        assert stub_one.isConnected();
        Util.waitUntil(5000, 500, () -> router_a.numRegisteredClients() == 1);
        stub_one.disconnect(GROUP, stub_one_addr);
        Util.waitUntil(5000, 500, () -> router_a.numRegisteredClients() == 0);
    }

    public void testRouterStubManager() throws Exception {
        stub_mgr=new RouterStubManager(LOG, timer, GROUP, Util.createRandomAddress("stubmgr-local"),
                                       "stubmgr-local", null, 1000);
        RouterStub stub_a=stub_mgr.createAndRegisterStub(null, new InetSocketAddress(BIND_ADDR, router_a.port()));
        RouterStub stub_b=stub_mgr.createAndRegisterStub(null, new InetSocketAddress(BIND_ADDR, router_b.port()));
        assert !stub_a.isConnected();
        assert !stub_b.isConnected();
        stub_mgr.connectStubs();
        Util.waitUntilTrue(2000, 100, () -> stub_a.isConnected() && stub_b.isConnected());
        assert stub_a.isConnected();
        assert stub_b.isConnected();
        router_b.stop();
        Util.waitUntilTrue(5000, 100, () -> stub_a.isConnected() && !stub_b.isConnected());
        assert stub_a.isConnected();
        assert !stub_b.isConnected();

        router_b.start(); // restarting B
        Util.waitUntilTrue(5000, 500, () -> stub_a.isConnected() && stub_b.isConnected());
        assert stub_a.isConnected();
        assert stub_b.isConnected();

    }
}
