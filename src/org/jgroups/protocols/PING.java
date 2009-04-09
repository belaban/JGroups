package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;
import org.jgroups.util.UUID;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;


/**
 * The PING protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by mcasting PING
 * requests to an IP MCAST address (or, if gossiping is enabled, by contacting the GossipRouter).
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipRouter, default is null
 * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipRouter, default is null
 * @author Bela Ban
 * @version $Id: PING.java,v 1.55 2009/04/09 18:09:28 vlada Exp $
 */
public class PING extends Discovery {
    
    private static final String name="PING";
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    
    @Property(description="Gossip host")
    private String gossip_host=null;
    
    @Property(description="Gossip port")
    private int gossip_port=0;
    
    @Property(description="Time in msecs after which the entry in GossipRouter will be refreshed. Default is 20000 msec")
    private long gossip_refresh=20000;
    
    @Property(description="Number of ports to be probed for initial membership. Default is 1")
    private int port_range=1;
   
    @Property(description="If socket is used for discovery, time in msecs to wait until socket is connected. Default is 1000 msec")
    private int socket_conn_timeout=1000;

    @Property(description="Max to block on the socket on a read (in ms). 0 means block forever")
    private int socket_read_timeout=3000;

    @Property(description="Time (in ms) to wait for our own discovery message to be received. 0 means don't wait. If the " +
            "discovery message is not received within discovery_timeout ms, a warning will be logged")
    private long discovery_timeout=0L;

    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    private List<RouterStub> clients = new ArrayList<RouterStub>();
    
    private List<InetSocketAddress> gossip_hosts=null;
        
    private List<IpAddress> initial_hosts=null; // hosts to be contacted for the initial membership

    protected final Promise<Boolean>   discovery_reception=new Promise<Boolean>();


    

    public String getName() {
        return name;
    }
    
    public void init() throws Exception {
        super.init();
    }

	private void initializeRouterStubs() {   
	    PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
		if (gossip_hosts != null) {		   
			for (InetSocketAddress host : gossip_hosts) {
				RouterStub rs = new RouterStub(host.getHostName(), host.getPort(), null, physical_addr);
				rs.setSocketConnectionTimeout(socket_conn_timeout);
				rs.setSocketReadTimeout(socket_read_timeout);
				clients.add(rs);
			}
		} else if (gossip_host != null && gossip_port != 0) {
			RouterStub rs = new RouterStub(gossip_host, gossip_port, null,physical_addr);
			rs.setSocketConnectionTimeout(socket_conn_timeout);
			rs.setSocketReadTimeout(socket_read_timeout);
			clients.add(rs);
		}
	}
	
	private boolean isUsingRouterStubs(){
	    return gossip_hosts != null || (gossip_host != null && gossip_port != 0);
	}


    public int getGossipPort() {
        return gossip_port;
    }

    public void setGossipPort(int gossip_port) {
        this.gossip_port=gossip_port;
    }

    public long getGossipRefresh() {
        return gossip_refresh;
    }

    public void setGossipRefresh(long gossip_refresh) {
        this.gossip_refresh=gossip_refresh;
    }

    public int getSocketConnTimeout() {
        return socket_conn_timeout;
    }
    
    public void setSocketConnTimeout(int socket_conn_timeout) {
        this.socket_conn_timeout=socket_conn_timeout;
    }

    public int getSocketReadTimeout() {
        return socket_read_timeout;
    }
    
    public void setSocketReadTimeout(int socket_read_timeout) {
        this.socket_read_timeout=socket_read_timeout;
    }

     public int getSockConnTimeout() {
        return socket_conn_timeout;
    }

    @Property
    public void setSockConnTimeout(int socket_conn_timeout) {
        this.socket_conn_timeout=socket_conn_timeout;
    }

    public int getSockReadTimeout() {
        return socket_read_timeout;
    }

    @Property
    public void setSockReadTimeout(int socket_read_timeout) {
        this.socket_read_timeout=socket_read_timeout;
    }

    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=Util.parseCommaDelimetedHosts(hosts, port_range);
    }

    @Property
    public void setGossipHosts(String hosts) throws UnknownHostException {
        gossip_hosts=Util.parseCommaDelimetedHosts2(hosts, port_range);
    }


    public void stop() {
        super.stop();
        for(RouterStub stub:clients){
        	stub.disconnect();
        }
        discovery_reception.reset();
    }
    
    public void destroy(){
        super.destroy();
        clients.clear();
    }


    public void handleConnect() {
    	for(RouterStub client:clients){
    		try {
				client.connect(group_addr);
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
    }

    public void handleDisconnect() {
    	for(RouterStub client:clients){
    		try {
				client.disconnect();
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}
    }



    public void sendGetMembersRequest(String cluster_name) {
        Message       msg;
        PingHeader    hdr;
        List<Address> gossip_rsps = new ArrayList<Address>();
        
        boolean routerStubsInitiliazed = !clients.isEmpty();
        if(isUsingRouterStubs() && !routerStubsInitiliazed){
            initializeRouterStubs();
        }

        if(!clients.isEmpty()) {
        	for(RouterStub client:clients){
        		try {
					gossip_rsps.addAll(client.getMembers(group_addr, 2500));
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
            if(!gossip_rsps.isEmpty()) {
                // Set a temporary membership in the UDP layer, so that the following multicast
                // will be sent to all of them
                Event view_event=new Event(Event.TMP_VIEW, makeView(new Vector<Address>(gossip_rsps)));
                down_prot.down(view_event); // needed e.g. by failure detector or UDP
            }
            else {
                //do nothing
                return;
            }

            if(!gossip_rsps.isEmpty()) {
                for(Iterator<Address> it=gossip_rsps.iterator(); it.hasNext();) {
                    Address dest=it.next();
                    msg=new Message(dest, null, null);  // unicast msg
                    msg.setFlag(Message.OOB);
                    msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name));
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }

            Util.sleep(500);
        }
        else {
            if(initial_hosts != null && !initial_hosts.isEmpty()) {
                for(Address addr: initial_hosts) {
                    if(addr.equals(local_addr))
                        continue;
                    // if(tmpMbrs.contains(addr)) {
                    // ; // continue; // changed as suggested by Mark Kopec
                    // }
                    msg=new Message(addr, null, null);
                    msg.setFlag(Message.OOB);
                    msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name));

                    if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }
            else {
                // 1. Mcast GET_MBRS_REQ message
                PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
                List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
                PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
                hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
                msg=new Message(null);  // mcast msg
                msg.setFlag(Message.OOB);
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                sendMcastDiscoveryRequest(msg);
            }
        }
    }


    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            PingHeader hdr=(PingHeader)msg.getHeader(getName());
            if(hdr != null && hdr.type == PingHeader.GET_MBRS_REQ && msg.getSrc().equals(local_addr)) {
                discovery_reception.setResult(true);
            }
        }

        return super.up(evt);
    }

    void sendMcastDiscoveryRequest(Message discovery_request) {
        discovery_reception.reset();
        down_prot.down(new Event(Event.MSG, discovery_request));
        waitForDiscoveryRequestReception();
    }


    protected void waitForDiscoveryRequestReception() {
        if(discovery_timeout > 0) {
            try {
                discovery_reception.getResultWithTimeout(discovery_timeout);
            }
            catch(TimeoutException e) {
                if(log.isWarnEnabled())
                    log.warn("didn't receive my own discovery request - multicast socket might not be configured correctly");
            }
        }
    }
}