package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.Route;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Various RELAY2-related tests
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class Relay2RpcDispatcherTest {
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

    @BeforeMethod
    protected void setUp() throws Exception {
    	a = createNode(LON, "A");
    	b = createNode(LON, "B");
    	al=new MyReceiver("A");
    	bl=new MyReceiver("B");
    	rpca = new RpcDispatcher(a, new ServerObject(1)).setMembershipListener(al);
    	rpcb = new RpcDispatcher(b, new ServerObject(1)).setMembershipListener(bl);
    	
    	x = createNode(SFO, "X");
    	y = createNode(SFO, "Y");
    	xl=new MyReceiver("X");
    	yl=new MyReceiver("Y");
    	rpcx = new RpcDispatcher(x, new ServerObject(1)).setMembershipListener(xl);
    	rpcy = new RpcDispatcher(y, new ServerObject(1)).setMembershipListener(yl);
    }
    @AfterMethod protected void destroy() {Util.close(y,x,b,a);}


    public void testRpcToUnknownSite() throws Exception {
        a.connect(LON_CLUSTER);
        try {
            rpca.callRemoteMethod(new SiteMaster("nyc"),"foo",null,null,RequestOptions.SYNC());
            assert false : "The RPC should have thrown an UnreachableException";
        }
        catch(UnreachableException unreachable) {
            System.out.println("caught " + unreachable.getClass().getSimpleName() + " - as expected");
        }
    }

    /**
     * Tests that notifications are routed to all sites.
     */
    public void testNotificationAndRpcRelay2Transit() throws Exception {
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

        RELAY2 ar=a.getProtocolStack().findProtocol(RELAY2.class);
        RELAY2 xr=x.getProtocolStack().findProtocol(RELAY2.class);

        assert ar != null && xr != null;

        JChannel a_bridge=null, x_bridge=null;
        for(int i=0; i < 20; i++) {
            a_bridge=ar.getBridge(SFO);
            x_bridge=xr.getBridge(LON);
            if(a_bridge != null && x_bridge != null && a_bridge.getView().size() == 2 && x_bridge.getView().size() == 2)
                break;
            Util.sleep(500);
        }

        assert a_bridge != null && x_bridge != null;

        System.out.println("A's bridge channel: " + a_bridge.getView());
        System.out.println("X's bridge channel: " + x_bridge.getView());
        assert a_bridge.getView().size() == 2 : "bridge view is " + a_bridge.getView();
        assert x_bridge.getView().size() == 2 : "bridge view is " + x_bridge.getView();

        Route route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        System.out.println("B: sending message 0 to the site master of SFO");
        Address sm_sfo=new SiteMaster(SFO);
        MethodCall call=new MethodCall(ServerObject.class.getMethod("foo"));
        System.out.println("B: call foo method on A");
        Object rsp = rpcb.callRemoteMethod(a.getAddress(), call, new RequestOptions(ResponseMode.GET_ALL,5000));
        System.out.println("RSP is: " + rsp );
        
        
        System.out.println("B: call foo method on SFO master site");
        rsp = rpcb.callRemoteMethod(sm_sfo, call, new RequestOptions(ResponseMode.GET_ALL,15000));
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

    private static void checkMsgDelivery(MyReceiver... rl) {
    	for(MyReceiver r : rl) {
	    	List<Integer> list=r.getList();
	        for(int i=0; i < 20; i++) {
	            if(!list.isEmpty())
	                break;
	            Util.sleep(500);
	        }
	        System.out.println("list = " + list);
	        assert list.size() == 1 && list.get(0) == 0;
	        r.clear();
    	}
    }
    
    
    protected JChannel createNode(String site_name, String node_name) throws Exception {
    	JChannel ch=new JChannel(new SHARED_LOOPBACK(),
    			new SHARED_LOOPBACK_PING(),
                new MERGE3().setValue("max_interval", 3000).setValue("min_interval", 1000),
    			new NAKACK2(),
    			new UNICAST3(),
    			new GMS().setValue("print_local_addr", false),
    			new FORWARD_TO_COORD(),
    			createRELAY2(site_name));
    	ch.setName(node_name);
    	return ch;
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


    protected RELAY2 createRELAY2(String site_name) {
        RELAY2 relay=new RELAY2().site(site_name).enableAddressTagging(false).asyncRelayCreation(true).relayMulticasts(true);

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
          new GMS().setValue("print_local_addr", false)
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


    protected void waitForBridgeView(int expected_size, long timeout, long interval, JChannel ... channels) {
        long deadline=System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < deadline) {
            boolean views_correct=true;
            for(JChannel ch: channels) {
                RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
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
            RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            System.out.println(ch.getAddress() + ": " + bridge_view);
        }

        for(JChannel ch: channels) {
            RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            assert bridge_view != null && bridge_view.size() == expected_size
              : ch.getAddress() + ": bridge view=" + bridge_view + ", expected=" + expected_size;
        }
    }


    protected Route getRoute(JChannel ch, String site_name) {
        RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
        return relay.getRoute(site_name);
    }


    protected static class MyReceiver extends ReceiverAdapter {
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
