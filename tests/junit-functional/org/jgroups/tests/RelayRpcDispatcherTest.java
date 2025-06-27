package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.relay.*;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Various RpcDispatcher tests for {@link org.jgroups.protocols.relay.RELAY2} and {@link RELAY3}
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="relayProvider")
public class RelayRpcDispatcherTest {
    protected JChannel a, b;  			// members in site "lon"
    protected JChannel x, y;  			// members in site "sfo
    
    protected RpcDispatcher rpca, rpcb;	// rpc in site "lon"
    protected RpcDispatcher rpcx, rpcy;	// rpc in site "sfo"
    
    protected MyReceiver al, bl;		// receiver in site "lon"
    protected MyReceiver xl, yl;		// receiver in site "sfo"
    
    protected static final String BRIDGE_CLUSTER = "global";
    protected static final String LON_CLUSTER    = "lon-cluster";
    protected static final String SFO_CLUSTER    = "sfo-cluster";
    protected static final String SFO            = "sfo", LON="lon";

    protected static final InetAddress LOOPBACK;

    static {
        LOOPBACK=InetAddress.getLoopbackAddress();
    }

    @DataProvider protected Object[][] relayProvider() {
        return new Object[][] {
          {RELAY2.class},
          {RELAY3.class}
        };
    }

    @Test(enabled=false)
    protected void setUp(Class<? extends RELAY> cl) throws Exception {
    	a = createNode(cl, LON, "A");
    	b = createNode(cl, LON, "B");
    	al=new MyReceiver("A");
    	bl=new MyReceiver("B");
    	rpca = new RpcDispatcher(a, new ServerObject(1)).setReceiver(al);
    	rpcb = new RpcDispatcher(b, new ServerObject(1)).setReceiver(bl);
    	
    	x = createNode(cl, SFO, "X");
    	y = createNode(cl, SFO, "Y");
    	xl=new MyReceiver("X");
    	yl=new MyReceiver("Y");
    	rpcx = new RpcDispatcher(x, new ServerObject(1)).setReceiver(xl);
    	rpcy = new RpcDispatcher(y, new ServerObject(1)).setReceiver(yl);
    }
    @AfterMethod protected void destroy() {Util.close(y,x,b,a);}


    public void testRpcToUnknownSite(Class<? extends RELAY> cl) throws Exception {
        setUp(cl);
        a.connect(LON_CLUSTER);
        try {
            rpca.callRemoteMethod(new SiteMaster("nyc"),"foo",null,null,RequestOptions.SYNC());
            assert false : "The RPC should have thrown an UnreachableException";
        }
        catch(UnreachableException unreachable) {
            System.out.println("caught " + unreachable.getClass().getSimpleName() + " - as expected");
        }
    }

    /** A:lon invokes a sync RPC on Y:sfo, but Y:sfo is dead. It should throw an exception */
    public void testMemberUnreachable(Class<? extends RELAY> cl) throws Exception {
        if(cl.equals(RELAY2.class))  // doesn't support MBR_UNREACHABLE event
            return;
        setUp(cl);
        a.connect(LON_CLUSTER);
        x.connect(SFO_CLUSTER);
        y.connect(SFO_CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(2000, 100, x,y);
        Address target=RelayTests.addr(cl, y, SFO);
        Util.close(y);
        Util.waitUntil(2000, 100, () -> y.getView() == null && x.getView().size() == 1);
        RELAY r=a.stack().findProtocol(RELAY.class);
        r.setRouteStatusListener(new DefaultRouteStatusListener(r::addr).verbose(true));
        try {
            rpca.callRemoteMethod(target,"foo",null,null,RequestOptions.SYNC());
            assert false : "The RPC should have thrown an UnreachableException";
        }
        catch(UnreachableException unreachable) {
            System.out.println("caught " + unreachable.getClass().getSimpleName() + " - as expected");
        }
    }

    /**
     * Tests that notifications are routed to all sites.
     */
    public void testNotificationAndRpcRelay2Transit(Class<? extends RELAY> cl) throws Exception {
        setUp(cl);
    	a.connect(LON_CLUSTER);
    	b.connect(LON_CLUSTER);
    	rpca.start();
    	rpcb.start();
    	Util.waitUntilAllChannelsHaveSameView(30000, 1000, a, b);
    	
    	x.connect(SFO_CLUSTER);
    	y.connect(SFO_CLUSTER);
    	rpcx.start();
    	rpcy.start();
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, x, y);

        assert a.getView().size() == 2;
        assert x.getView().size() == 2;

        RELAY ar=a.getProtocolStack().findProtocol(RELAY.class);
        RELAY xr=x.getProtocolStack().findProtocol(RELAY.class);

        assert ar != null && xr != null;
        Util.waitUntil(10000, 500, () -> {
            JChannel ab=ar.getBridge(SFO); JChannel xb=xr.getBridge(LON);
            return ab != null && xb != null && ab.getView().size() == 2 && xb.getView().size() == 2;
        });
        JChannel a_bridge=ar.getBridge(SFO), x_bridge=xr.getBridge(LON);
        System.out.println("A's bridge channel: " + a_bridge.getView());
        System.out.println("X's bridge channel: " + x_bridge.getView());

        Route route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        assertSiteView(a, List.of(SFO, LON));
        assert getCurrentSites(b) == null;

        assertSiteView(x, List.of(LON, SFO));
        assert getCurrentSites(y) == null;

        MethodCall call=new MethodCall(ServerObject.class.getMethod("foo"));
        System.out.println("B: call foo method on A");
        Object rsp = rpcb.callRemoteMethod(a.getAddress(), call, new RequestOptions(ResponseMode.GET_ALL, 2000));
        System.out.println("RSP is: " + rsp );
        
        System.out.println("B: sending message 0 to the site master of SFO");
        Address sm_sfo=new SiteMaster(SFO);
        System.out.println("B: call foo method on SFO master site");
        rsp = rpcb.callRemoteMethod(sm_sfo, call, new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("RSP is: " + rsp );
        
        System.out.println("B: call foo method on all members in site LON");
        RspList<Integer> rsps = rpcb.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL,5000));
        System.out.println("RSPs are: \n" + rsps);
        assert rsps.size() == 2;
        assert rsps.containsKey(a.getAddress()) && rsps.containsKey(b.getAddress());
        
        View bridge_view=xr.getBridgeView(BRIDGE_CLUSTER);
        System.out.println("bridge_view = " + bridge_view);

        route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;
    }
    
    
    protected static JChannel createNode(Class<? extends RELAY> cl, String site_name, String node_name) throws Exception {
        Protocol[] protocols={
          new TCP().setBindAddress(LOOPBACK),
          new LOCAL_PING(),
          new NAKACK2().useMcastXmit(false),
          cl.equals(RELAY3.class)? createRELAY(cl, site_name).asyncRelayCreation(false) : null,
          new UNICAST3(),
          new STABLE().setDesiredAverageGossip(50000).setMaxBytes(8_000_000),
          new GMS().printLocalAddress(false).setJoinTimeout(100),
          cl.equals(RELAY2.class)? createRELAY(cl, site_name) : null,
        };
        return new JChannel(Util.combine(Protocol.class, protocols)).name(node_name);
    }


    protected static class ServerObject {
    	protected int i;

    	public ServerObject(int i) {
    		this.i=i;
    	}
    	public int foo() {
            System.out.println("foo()");
            return i;
        }
    	
    	public static long sleep(long timeout) {
    		long start=System.currentTimeMillis();
    		Util.sleep(timeout);
    		return System.currentTimeMillis() - start;
    	}
    }


    protected static RELAY createRELAY(Class<? extends RELAY> cl, String site_name) throws Exception {
        if(cl == null)
            return null;
        RELAY relay=cl.getConstructor().newInstance();
        relay.site(site_name).asyncRelayCreation(true);

        RelayConfig.SiteConfig lon_cfg=new RelayConfig.SiteConfig(LON),
          sfo_cfg=new RelayConfig.SiteConfig(SFO);

        lon_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));
        sfo_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));
        relay.addSite(LON, lon_cfg).addSite(SFO, sfo_cfg);
        return relay;
    }

    protected static Protocol[] createBridgeStack() {
        return new Protocol[]{
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3(),
          new GMS().printLocalAddress(false)
        };
    }

    /** Creates a singleton view for each channel listed and injects it */
    protected static void createPartition(JChannel ... channels) {
        for(JChannel ch: channels) {
            View view=View.create(ch.getAddress(), 5, ch.getAddress());
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }


    protected static void waitForBridgeView(int expected_size, long timeout, long interval, JChannel... channels) {
        long deadline=System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < deadline) {
            boolean views_correct=true;
            for(JChannel ch: channels) {
                RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
                View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
                if(bridge_view == null || bridge_view.size() != expected_size) {
                    views_correct=false;
                    break;
                }
            }
            if(views_correct)
                break;
            Util.sleep(interval);
        }

        System.out.println("Bridge views:\n");
        for(JChannel ch: channels) {
            RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            System.out.println(ch.getAddress() + ": " + bridge_view);
        }

        for(JChannel ch: channels) {
            RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            assert bridge_view != null && bridge_view.size() == expected_size
              : ch.getAddress() + ": bridge view=" + bridge_view + ", expected=" + expected_size;
        }
    }


    protected static Route getRoute(JChannel ch, String site_name) {
        RELAY relay=ch.getProtocolStack().findProtocol(RELAY.class);
        return relay.getRoute(site_name);
    }

    protected static Collection<String> getCurrentSites(JChannel channel) {
       RELAY relay=channel.getProtocolStack().findProtocol(RELAY.class);
       return relay.getCurrentSites();
    }

    protected static void assertSiteView(JChannel channel, Collection<String> siteNames) {
       Collection<String> sites=getCurrentSites(channel);
       assert sites != null;
       assert sites.size() == siteNames.size();
       for(String site: siteNames)
          assert sites.contains(site);
    }

    protected static class MyReceiver implements Receiver {
        protected final List<Integer> list=new ArrayList<>(5);
        String chName;
        MyReceiver(String chName) {
        	this.chName = chName;
        }
        public List<Integer> getList()            {return list;}
        public void          clear()              {list.clear();}

        public void          receive(Message msg) {
            list.add(msg.getObject());
            System.out.println(chName + "<-- " + msg.getObject());
        }
    }

}
