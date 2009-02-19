// $Id: TUNNEL.java,v 1.57 2009/02/19 17:49:20 vlada Exp $

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.Property;
import org.jgroups.stack.RouterStub;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is
 * opened to a Router (using the RouterStub client-side stub), the IP
 * address/port of which was given using channel properties
 * <code>router_host</code> and <code>router_port</code>. All outgoing
 * traffic is sent via this TCP socket to the Router which distributes it to all
 * connected TUNNELs in this group. Incoming traffic received from Router will
 * simply be passed up the stack.
 * 
 * <p>
 * A TUNNEL layer can be used to penetrate a firewall, most firewalls allow
 * creating TCP connections to the outside world, however, they do not permit
 * outside hosts to initiate a TCP connection to a host inside the firewall.
 * Therefore, the connection created by the inside host is reused by Router to
 * send traffic from an outside host to a host inside the firewall.
 * 
 * @author Bela Ban
 */
public class TUNNEL extends TP {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
	@Deprecated
    @Property(name="router_host",deprecatedMessage="router_host is deprecated. Specify target GRs using gossip_router_hosts",description="Router host address")
    private String router_host = null;

	@Deprecated
    @Property(name="router_port",deprecatedMessage="router_port is deprecated. Specify target GRs using gossip_router_hosts",description="Router port")
    private int router_port = 0;    

    @Property(description="Interval in msec to attempt connecting back to router in case of torn connection. Default is 5000 msec")
    private long reconnect_interval = 5000;
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
	private final List<InetSocketAddress> gossip_router_hosts = new ArrayList<InetSocketAddress>();
	
	private final List<RouterStub> stubs = new ArrayList<RouterStub>();
	
	private TUNNELPolicy tunnel_policy = new DefaultTUNNELPolicy();

    /** time to wait in ms between reconnect attempts */

    @GuardedBy("reconnectorLock") 
    private final Map<InetSocketAddress,Future<?>> reconnectFutures = new HashMap<InetSocketAddress, Future<?>>();

    private final Lock reconnectorLock = new ReentrantLock();

    public TUNNEL(){}
    
    @Property
    public void setGossipRouterHosts(String hosts) throws UnknownHostException {
    	gossip_router_hosts.clear();
    	gossip_router_hosts.addAll(Util.parseCommaDelimetedHosts2(hosts,1));       
    }

    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ')';
    }

    @Deprecated
    public String getRouterHost() {
        return router_host;
    }

    @Deprecated
    public void setRouterHost(String router_host) {
        this.router_host=router_host;
    }

    @Deprecated
    public int getRouterPort() {
        return router_port;
    }

    @Deprecated
    public void setRouterPort(int router_port) {
        this.router_port=router_port;
    }

    public long getReconnectInterval() {
        return reconnect_interval;
    }

    public void setReconnectInterval(long reconnect_interval) {
        this.reconnect_interval=reconnect_interval;
    }

    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "TUNNEL";
    }
    
    public void setTUNNELPolicy(TUNNELPolicy policy) {
    	if(policy == null) throw new IllegalArgumentException("Tunnel policy has to be non null");
		tunnel_policy = policy;
	}

    public void init() throws Exception {        
        super.init();
        if(timer == null)
            throw new Exception("TUNNEL.init(): timer cannot be retrieved from protocol stack");

        if((router_host == null || router_port == 0) && gossip_router_hosts.isEmpty()){
            throw new Exception("Either router_host and router_port have to be set or a list of gossip routers");            
        }
        
        if(router_host != null && router_port != 0 && !gossip_router_hosts.isEmpty()){ 
        	throw new Exception("Cannot specify both router host and port along with gossip_router_hosts");
        }
        
        if(router_host != null && router_port != 0 && gossip_router_hosts.isEmpty()){
        	gossip_router_hosts.add(new InetSocketAddress(router_host,router_port));
        }
        
        if(log.isDebugEnabled()){
            log.debug("Target GRs are:" + gossip_router_hosts.toString());
        }
    }

    public void start() throws Exception {
        // loopback turned on is mandatory
        loopback = true;

        for(InetSocketAddress gr:gossip_router_hosts){
        	RouterStub stub = new RouterStub(gr.getHostName(), gr.getPort(), bind_addr);
            stub.setConnectionListener(new StubConnectionListener(stub));
        	stubs.add(stub);
        	if(local_addr == null){
        		local_addr = stub.getLocalAddress();
                if(additional_data != null && local_addr instanceof IpAddress)
                    ((IpAddress) local_addr).setAdditionalData(additional_data);
        	}
        }
        super.start();
    }

    public void stop() {        
        teardownTunnel();
        super.stop();        
        local_addr = null;
    }

    void teardownTunnel() {
        for(RouterStub stub:stubs){
        	stopReconnecting(stub);
        	stub.disconnect();
        }
    }

    public Object handleDownEvent(Event evt) {
        Object retEvent = super.handleDownEvent(evt);
        switch(evt.getType()){
        case Event.CONNECT:
        case Event.CONNECT_WITH_STATE_TRANSFER:
	        tunnel_policy.connect(stubs);
            break;

        case Event.DISCONNECT:
            teardownTunnel();
            break;
        }
        return retEvent;
    }

    private void startReconnecting(final RouterStub stub) {
        reconnectorLock.lock();
        try{
        	Future<?>reconnectorFuture = reconnectFutures.get(stub.getGossipRouterAddress());
            if(reconnectorFuture == null || reconnectorFuture.isDone()){
                final Runnable reconnector = new Runnable() {
                    public void run() {
                        try{
                            if(!stub.isIntentionallyDisconnected()){
                                if(log.isDebugEnabled()){
                                    log.debug("Reconnecting " + getLocalAddress()
                                              + " to router at "
                                              + stub.getGossipRouterAddress());
                                }
                                stub.connect(channel_name);
                            }
                        } catch (Exception ex) {
							if (log.isWarnEnabled())
								try {
									log.warn("failed reconnecting "
											+ stub.getLocalAddress() + " to GR at "
											+ stub.getGossipRouterAddress(), ex);
								} catch (SocketException e) {
								}
						}
                    }
                };
                reconnectorFuture = timer.scheduleWithFixedDelay(reconnector,
                                                                 0,
                                                                 reconnect_interval,
                                                                 TimeUnit.MILLISECONDS);
                reconnectFutures.put(stub.getGossipRouterAddress(), reconnectorFuture);
            }
        }finally{
            reconnectorLock.unlock();
        }
    }

    private void stopReconnecting(final RouterStub stub) {
        reconnectorLock.lock();
        InetSocketAddress address = stub.getGossipRouterAddress();
        try{
        	Future<?>reconnectorFuture = reconnectFutures.get(address);
            if(reconnectorFuture != null){
                reconnectorFuture.cancel(true);
                reconnectFutures.remove(address);
            }
        }finally{
            reconnectorLock.unlock();
        }
    }

    private class StubConnectionListener implements RouterStub.ConnectionListener {

    	private final RouterStub stub;
        public StubConnectionListener(RouterStub stub) {
			super();
			this.stub = stub;
		}

		private volatile int currentState = RouterStub.STATUS_DISCONNECTED;

        public void connectionStatusChange(int newState) {            
            if(newState == RouterStub.STATUS_DISCONNECTED){
                startReconnecting(stub);
            }else if(currentState != RouterStub.STATUS_CONNECTED && newState == RouterStub.STATUS_CONNECTED){
                stopReconnecting(stub);
                Thread t = global_thread_factory.newThread(new StubReceiver(stub), "TUNNEL receiver");
                t.setDaemon(true);
                t.start();
            }
            currentState = newState;
        }
    }

    private class StubReceiver implements Runnable {
    	private final RouterStub stub;
        public StubReceiver(RouterStub stub) {
			super();
			this.stub = stub;			
		}

		public void run() {
            while(stub.isConnected()){
                Address dest = null;                
                int len;
                byte[] data = null;
                DataInputStream input = null;
                try{
                    input = stub.getInputStream();
                    dest = Util.readAddress(input);                    
                    len = input.readInt();
                    if(len > 0){
                        data = new byte[len];
                        input.readFully(data, 0, len);                        
                        receive(dest, null/*src will be read from data*/, data, 0, len);
                    }
                }catch(SocketException se){
                    // if(log.isWarnEnabled()) log.warn("failure in TUNNEL
                    // receiver thread", se);
                }catch(IOException ioe){
                    // if(log.isWarnEnabled()) log.warn("failure in TUNNEL
                    // receiver thread", ioe);
                }catch(Exception e){
                    if(log.isWarnEnabled())
                        log.warn("failure in TUNNEL receiver thread", e);
                }
            }
        }
    }

    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        tunnel_policy.sendToAllMembers(stubs, data, offset, length);
    }

    public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
        tunnel_policy.sendToSingleMember(stubs,dest, data, offset, length);
    }

    public String getInfo() {
        if(stubs.isEmpty())
            return stubs.toString();
        else
            return "RouterStubs not yet initialized";
    }

    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
        msg.setDest(dest);
    }

    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
        msg.setDest(dest);
    }
    
    public interface TUNNELPolicy{
    	public void connect(List<RouterStub> stubs);
    	public void sendToAllMembers(List<RouterStub> stubs, byte[]data,int offset,int length);
    	public void sendToSingleMember(List<RouterStub> stubs, Address dest, byte[]data,int offset,int length);
    }
    
    private class DefaultTUNNELPolicy implements TUNNELPolicy {

		public void sendToAllMembers(List<RouterStub> stubs, byte[] data,
				int offset, int length) {
			for (RouterStub stub : stubs) {
				try {
					stub.sendToAllMembers(data, offset, length);
					if (log.isDebugEnabled())
						log.debug(stub.getLocalAddress()
								+ " sent a message to all members, GR used "
								+ stub.getGossipRouterAddress());
					break;
				} catch (Exception e) {
					try {
						log.warn(stub.getLocalAddress()
								+ " failed sending a message to all members, GR used "
								+ stub.getGossipRouterAddress());
					} catch (SocketException e1) {
					}
				}
			}

		}

		public void sendToSingleMember(List<RouterStub> stubs, Address dest,
				byte[] data, int offset, int length) {
			for (RouterStub stub : stubs) {
				try {
					stub.sendToSingleMember(dest, data, offset, length);
					if (log.isDebugEnabled())
						log.debug(stub.getLocalAddress()
								+ " sent a message to " + dest + ", GR used "
								+ stub.getGossipRouterAddress());
					break;
				} catch (Exception e) {
					try {
						log.warn(stub.getLocalAddress()
								+ " failed sending a message to " + dest
								+ ", GR used " + stub.getGossipRouterAddress());
					} catch (SocketException e1) {
					}
				}
			}
		}

		public void connect(List<RouterStub> stubs) {
			for (RouterStub stub : stubs) {
				try {
					stub.connect(channel_name);
					if (log.isDebugEnabled())
						log.debug("Connected to GR at " + stub.getGossipRouterAddress());
				} catch (Exception e) {
					if (log.isErrorEnabled())
						log.warn("Failed connecting to GossipRouter at " + stub.getGossipRouterAddress());
					startReconnecting(stub);
				}
			}
		}
	}
}
