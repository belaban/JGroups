
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 *
 * @version $Id: TUNNEL_Test2.java,v 1.1 2009/02/16 18:54:12 vlada Exp $
 **/
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class TUNNEL_Test2 extends ChannelTestBase{
    private JChannel channel, coordinator;
    private final static String GROUP="TUNNEL_Test2";
    private GossipRouter gossipRouter,gossipRouter2;
    private static final String props="tunnel2.xml";

    @BeforeClass
    void startRouter() throws Exception {
        gossipRouter=new GossipRouter(12001);
        gossipRouter.start();
        
        gossipRouter2=new GossipRouter(12002);
        gossipRouter2.start();
    }
    
    @AfterClass
    void stopRouter() throws Exception {
        gossipRouter.stop();
        gossipRouter2.stop();
        
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
    }
    
    public void testSimpleConnect() throws Exception{
    	 channel = new JChannel(props);
         channel.connect(GROUP);
         assert channel.getLocalAddress() != null;
         channel.disconnect();
         assert channel.getLocalAddress() == null;
    }
    
    /**
     * Tests connect with two members
     * 
     **/
    public void testConnectTwoChannels() throws Exception {
        coordinator=new JChannel(props);
   
        channel=new JChannel(props);
        coordinator.connect(GROUP);
        channel.connect(GROUP);
        View view=channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());
    }
}
