package org.jgroups.stack;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.util.Util;

/**
 * Client stub that talks to a remote GossipRouter
 * 
 * @author Bela Ban
 * @version $Id: RouterStub.java,v 1.28 2007/05/09 22:57:50 belaban Exp $
 */
public class RouterStub {
	
	public final static int STATUS_CONNECTED = 0;
	public final static int STATUS_DISCONNECTED = 1;
	public final static int STATUS_CONNECTION_LOST = 2;

	String router_host = null; // name of the router host

	int router_port = 0; // port on which router listens on router_host

	Socket sock = null; // socket connecting to the router

	DataOutputStream output = null; // output stream associated with sock

	DataInputStream input = null; // input stream associated with sock

	Address local_addr = null; // addr of group mbr. Once assigned, remains the same
	
	private int connectionState = STATUS_DISCONNECTED;

	protected static final Log log = LogFactory.getLog(RouterStub.class);

	protected ConnectionListener conn_listener;

	private String groupname = null;

	private InetAddress bind_addr = null;

	public interface ConnectionListener {		
		void connectionStatusChange(int state);
	}

	/**
	 * Creates a stub for a remote Router object.
	 * 
	 * @param routerHost
	 *            The name of the router's host
	 * @param routerPort
	 *            The router's port
	 */
	public RouterStub(String routerHost,int routerPort,InetAddress bindAddress){
		router_host = routerHost != null ? routerHost : "localhost";
		router_port = routerPort;
		bind_addr = bindAddress;
	}
	
	public synchronized boolean isConnected() {
		return connectionState == STATUS_CONNECTED;
	}

	public void setConnectionListener(ConnectionListener conn_listener) {
		this.conn_listener = conn_listener;
	}

	public synchronized Address getLocalAddress() throws SocketException {
		if(local_addr == null){
			DatagramSocket my_sock = new DatagramSocket(0, bind_addr);
			local_addr = new IpAddress(bind_addr, my_sock.getLocalPort());
			Util.close(my_sock);
		}
		return local_addr;
	}
	
	/**
	 * Register this process with the router under <code>groupname</code>.
	 * 
	 * @param groupname
	 *            The name of the group under which to register
	 */
	public synchronized void connect(String groupname) throws Exception {
		if(groupname == null || groupname.length() == 0)
			throw new Exception("groupname is null");

		if(!isConnected()){
			this.groupname = groupname;
			try{
				sock = new Socket(router_host, router_port, bind_addr, 0);
				sock.setSoLinger(true, 500);
				output = new DataOutputStream(sock.getOutputStream());
				GossipData req = new GossipData(GossipRouter.CONNECT, groupname, getLocalAddress(),null);
				req.writeTo(output);
				output.flush();
				input = new DataInputStream(sock.getInputStream());
				connectionStateChanged(STATUS_CONNECTED);
			}catch(Exception e){
				if(log.isWarnEnabled())
					log.warn(this + " failed connecting to " + router_host + ":" + router_port);
				Util.close(sock);
				Util.close(input);
				Util.close(output);
				connectionStateChanged(STATUS_CONNECTION_LOST);
				throw e;
			}
		}
	}

	public synchronized void disconnect() {
		if(isConnected()){
			try{
				GossipData req = new GossipData(GossipRouter.DISCONNECT, groupname, local_addr,null);
				req.writeTo(output);
				output.flush();
			}catch(Exception e){
			}finally{
				Util.close(output);
				Util.close(input);
				Util.close(sock);
				sock = null;
				connectionStateChanged(STATUS_DISCONNECTED);
			}
		}
	}

	public String toString() {
		return "RouterStub[local_address=" + local_addr + ",router_host=" + router_host
				+ ",router_port=" + router_port + ",connected=" + isConnected() + "]";
	}

	public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
		// null destination represents mcast
		sendToSingleMember(null, data, offset, length);
	}

	public synchronized void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
		if(isConnected()){
			try{
				// 1. Group name
				output.writeUTF(groupname);

				// 2. Destination address (null in case of mcast)
				Util.writeAddress(dest, output);

				// 3. Length of byte buffer
				output.writeInt(data.length);

				// 4. Byte buffer
				output.write(data, 0, data.length);

				output.flush();

			}catch(SocketException se){
				if(log.isWarnEnabled())
					log.warn("Router stub " + this + " did not send message to "
							+ (dest == null ? "mcast" : dest + " since underlying socket is closed"));
				connectionStateChanged(STATUS_CONNECTION_LOST);
			}catch(Exception e){
				if(log.isErrorEnabled())
					log.error("Router stub " + this + " failed sending message to router");
				connectionStateChanged(STATUS_CONNECTION_LOST);
				throw new Exception("dest=" + dest + " (" + length + " bytes)", e);
			}
		}
	}

	public synchronized DataInputStream getInputStream() throws IOException {
		if(!isConnected()){
			throw new IOException("InputStream is closed");
		}
		return input;
	}
	
	private synchronized void connectionStateChanged(int newState) {
		boolean notify = connectionState != newState;
		connectionState = newState;
		if(notify && conn_listener != null){
			try{
				conn_listener.connectionStatusChange(newState);
			}catch(Throwable t){
				log.error("failed notifying ConnectionListener " + conn_listener, t);
			}
		}
	}
}
