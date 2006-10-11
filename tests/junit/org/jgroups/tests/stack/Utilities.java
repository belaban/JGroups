// $Id: Utilities.java,v 1.9 2006/10/11 08:01:12 belaban Exp $

package org.jgroups.tests.stack;

import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.GossipData;
import org.jgroups.util.Util;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Utility functions shared by stack tests.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.9 $
 * @since 2.2.1
 */
public class Utilities {

    static Log log=LogFactory.getLog(Utilities.class);


    private static GossipRouter gossipRouter=null;

    public static int startGossipRouter() throws Exception {
        return startGossipRouter(GossipRouter.EXPIRY_TIME,
                                 GossipRouter.GOSSIP_REQUEST_TIMEOUT,
                                 GossipRouter.ROUTING_CLIENT_REPLY_TIMEOUT);
    }

    public static int startGossipRouter(long expiryTime) throws Exception {
        return startGossipRouter(expiryTime,
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
                                        final long gossipRequestTimeout,
                                        final long routingClientReplyTimeout) throws Exception {

        if(gossipRouter != null)
            throw new Exception("GossipRouter already started");

        final int routerPort=getFreePort();
        try {
            gossipRouter=new GossipRouter(routerPort, null, expiryTime, gossipRequestTimeout, routingClientReplyTimeout);
            gossipRouter.start();
        }
        catch(Exception e) {
            log.error("Failed to start the router on port " + routerPort);
            gossipRouter=null;
            throw e;
        }

        DataInputStream dis=null;
        DataOutputStream dos=null;

        // verify the router - try for 10 secs to connect
        long startms=System.currentTimeMillis();
        Exception lastConnectException=null;
        long crtms=startms;
        Socket sock=null;

        try {
        while(crtms - startms < 10000) {
            try {
                sock=new Socket("localhost", routerPort);
                lastConnectException=null;
                break;
            }
            catch(Exception e) {
                lastConnectException=e;
                Thread.sleep(1000);
                crtms=System.currentTimeMillis();
            }
        }

        if(lastConnectException != null) {
            lastConnectException.printStackTrace();
            throw new Exception("Cannot connect to the router");
        }

        dis=new DataInputStream(sock.getInputStream());
        dos=new DataOutputStream(sock.getOutputStream());

        // verify the router - try for 10 sec to send/receive
        // read the IpAddress
        Util.readAddress(dis);
        // write a GET
        GossipData req=new GossipData(GossipRouter.GET, "nogroup_setup", null, null);
        req.writeTo(dos);
        GossipData rsp=new GossipData();
        rsp.readFrom(dis);
        System.out.println("router ok");
        }
        finally {
            Util.close(dos);
            Util.close(dis);
            Util.close(sock);
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
