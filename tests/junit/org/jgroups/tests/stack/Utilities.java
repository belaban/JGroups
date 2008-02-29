// $Id: Utilities.java,v 1.11 2008/02/29 12:20:06 belaban Exp $

package org.jgroups.tests.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;

import java.net.ServerSocket;

/**
 * Utility functions shared by stack tests.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.11 $
 * @since 2.2.1
 */
public class Utilities {

    static Log log=LogFactory.getLog(Utilities.class);


    private static GossipRouter gossipRouter=null;

    public static int startGossipRouter() throws Exception {
        return startGossipRouter(GossipRouter.EXPIRY_TIME,
                                 "localhost",
                                 GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                                 GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT);
    }


    public static int startGossipRouter(String bind_addr) throws Exception {
        return startGossipRouter(GossipRouter.EXPIRY_TIME,
                                 bind_addr,
                                 GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                                 GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT);
    }


    public static int startGossipRouter(long expiryTime) throws Exception {
        return startGossipRouter(expiryTime,
                                 "localhost",
                                 GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                                 GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT);
    }


    public static int startGossipRouter(long expiryTime, String bind_addr) throws Exception {
        return startGossipRouter(expiryTime,
                                 bind_addr,
                                 GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                                 GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    /**
     * Starts the router on a separate thread and makes sure it answers a dummy
     * GossipRouter.GET request.
     *
     * @return the port GossipRouter is listening on.
     */
    public static int startGossipRouter(final long expiryTime,
                                        final String bind_addr,
                                        final long gossipRequestTimeout,
                                        final long routingClientReplyTimeout) throws Exception {

        if(gossipRouter != null)
            throw new Exception("GossipRouter already started");

        final int routerPort=getFreePort();
        try {
            gossipRouter=new GossipRouter(routerPort, bind_addr, expiryTime, gossipRequestTimeout, routingClientReplyTimeout);
            gossipRouter.start();
        }
        catch(Exception e) {
            log.error("Failed to start the router on port " + routerPort);
            gossipRouter=null;
            throw e;
        }

        GossipClient client=null;

        // verify the router - try for 10 secs to connect
        long startms=System.currentTimeMillis();
        Exception lastConnectException=null;
        long crtms=startms;

        while(crtms - startms < 10000) {
            try {
                client=new GossipClient(new IpAddress(bind_addr, routerPort), 10000, 1000, null);
                client.getMembers("Utilities:startGossipRouterConnectionTest");
                lastConnectException=null;
                break;
            }
            catch(Exception e) {
                if(client != null)
                    client.stop();
                lastConnectException=e;
                Thread.sleep(1000);
                crtms=System.currentTimeMillis();
            }
        }

        if(lastConnectException != null) {
            lastConnectException.printStackTrace();
            throw new Exception("Cannot connect to the router");
        }
        return routerPort;
    }


    public static void stopGossipRouter() throws Exception {
        if(gossipRouter == null) {
            throw new Exception("There's no GossipRouter running");
        }
        gossipRouter.stop();
        System.out.println("router stopped");
        gossipRouter=null;
    }



    /**
     * Returns a port we can bind to.
     */
    public static int getFreePort() throws Exception {
        ServerSocket ss=new ServerSocket(0);
        int port=ss.getLocalPort();
        ss.close();
        return port;
    }

}
