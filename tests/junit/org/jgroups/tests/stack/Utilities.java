// $Id: Utilities.java,v 1.3 2004/03/30 06:47:30 belaban Exp $

package org.jgroups.tests.stack;

import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.GossipServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Utility functions shared by stack tests.
 *
 * @since 2.2.1
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.3 $
 **/
public class Utilities {


    private static GossipRouter gossipRouter = null;

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
     **/
    public static int startGossipRouter(final long expiryTime,
                                  final long gossipRequestTimeout,
                                  final long routingClientReplyTimeout) 
        throws Exception {

        if (gossipRouter!=null) {
            throw new Exception("GossipRouter already started");
        }

	final int routerPort = getFreePort();
	Thread routerThread = new Thread(new Runnable() {
		public void run() {	
		    try {
                        gossipRouter = 
                            new GossipRouter(routerPort, null, expiryTime,
                                             gossipRequestTimeout, 
                                             routingClientReplyTimeout);
                        gossipRouter.start();
		    }
		    catch(Exception e) {
                        String msg = 
                            "Failed to start the router on port "+routerPort;
			System.err.println(msg);
			e.printStackTrace();
                        gossipRouter = null;
		    }
		}});
	routerThread.start();

	// verify the router - try for 10 secs to connect
	long startms = System.currentTimeMillis();
	Exception lastConnectException = null;
	long crtms = startms;
        Socket s = null;

	while(crtms - startms < 10000) {
	    try {
		s = new Socket("localhost", routerPort);
                lastConnectException = null;
                break;
	    }
	    catch(Exception e) {
		lastConnectException = e;
                Thread.currentThread().sleep(1000);
		crtms = System.currentTimeMillis();
	    }
        }

	if (lastConnectException!=null) {
	    lastConnectException.printStackTrace();
            throw new Exception("Cannot connect to the router");
	}
	 
        final DataInputStream dis = new DataInputStream(s.getInputStream());
        final DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        // verify the router - try for 10 sec to send/receive
        final Object[] lock = new Object[1];
        synchronized(lock) {
            new Thread(new Runnable() {
                    public void run() {
                        Object result = null;
                        try {
                            // read the IpAddress
                            int len = dis.readInt();
                            byte[] buffer = new byte[len];
                            dis.read(buffer,0,len);
                            // write a GET
                            dos.writeInt(GossipRouter.GET);
                            dos.writeUTF("nogroup_setup");
                            dis.readInt();
                            result = new Object();
                        }
                        catch(Exception e) {
                            result = e;
                        }
                        finally {
                            synchronized(lock) {
                                lock[0] = result;
                                lock.notify();
                            }
                        }
                    }
                }).start();

            try {
                lock.wait(10000);
            }
            catch(InterruptedException e) {
            }
            if (lock[0]==null) {
                String msg =
                    "Cannot send/receive to/from the router: timeout";
                throw new Exception(msg);
            }
            else if (lock[0] instanceof Exception) {
                String msg =
                    "Cannot send/receive to/from the router: "+
                    ((Exception)lock[0]).getMessage();
                throw new Exception(msg);
            }
        }
        s.close();
	System.out.println("router ok");
	return routerPort;

    }


    public static void stopGossipRouter() throws Exception {
        
        if (gossipRouter==null) {
            throw new Exception("There's no GossipRouter running");
        }
        gossipRouter.stop();
        gossipRouter=null;
    }


    /**
     * Starts the GossipServer on a separate thread and sends a null GossipData
     *
     * @deprecated Since 2.2.1 GossipServer shouldn't be used anymore. The
     *             GossipRouter can server Gossip requests too.
     *
     * @return the port GossipServer is listening on.
     **/
    public static int startGossipServer(final long expiryTime)
        throws Exception {

	final int port = getFreePort();
	Thread gossipThread = new Thread(new Runnable() {
		public void run() {	
		    try {
			new GossipServer(port, expiryTime).run();
		    }
		    catch(Exception e) {
                        String msg = 
                            "Failed to start the gossip server on port "+port;
			System.err.println(msg);
			e.printStackTrace();
		    }
		}});
	gossipThread.start();

	// verify the server - try to connect for 10 secs
	long startms = System.currentTimeMillis();
	Exception lastConnectException = null;
	long crtms = startms;
        Socket s = null;

	while(crtms - startms < 10000) {
	    try {
		s = new Socket("localhost", port);
                lastConnectException = null;
                break;
	    }
	    catch(Exception e) {
		lastConnectException = e;
                Thread.currentThread().sleep(1000);
		crtms = System.currentTimeMillis();
	    }
        }

	if (lastConnectException!=null) {
	    lastConnectException.printStackTrace();
            throw new Exception("Cannot connect to the gossip server");
	}
	 
        // send a null GossipData which will be silently discared
        ObjectOutputStream oos= new ObjectOutputStream(s.getOutputStream());
        oos.writeObject(null);
        oos.close();
        s.close();
	System.out.println("gossip server ok");
	return port;

    }


    /**
     * Stops the GossipServer.
     *
     * @deprecated Since 2.2.1 GossipServer shouldn't be used anymore. The
     *             GossipRouter can server Gossip requests too.
     *
     **/
    public static void stopGossipServer(int port) throws Exception {
    }



    /**
     * Returns a port we can bind to.
     **/
    public static int getFreePort() throws Exception {
	ServerSocket ss = new ServerSocket(0);
	int port = ss.getLocalPort();
	ss.close();
	return port;
    }

}
