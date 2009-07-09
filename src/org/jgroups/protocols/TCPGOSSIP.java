
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;
import org.jgroups.util.Tuple;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;


/**
 * The TCPGOSSIP protocol layer retrieves the initial membership (used by the
 * GMS when started by sending event FIND_INITIAL_MBRS down the stack). We do
 * this by contacting one or more GossipRouters, which must be running at
 * well-known addresses:ports. The responses should allow us to determine the
 * coordinator whom we have to contact, e.g. in case we want to join the group.
 * When we are a server (after having received the BECOME_SERVER event), we'll
 * respond to TCPGOSSIP requests with a TCPGOSSIP response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * 
 * @author Bela Ban
 * @version $Id: TCPGOSSIP.java,v 1.41 2009/07/09 13:59:40 belaban Exp $
 */
@DeprecatedProperty(names={"gossip_refresh_rate"})
public class TCPGOSSIP extends Discovery implements RouterStub.ConnectionListener {
    
    private final static String name="TCPGOSSIP";    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Max time for socket creation. Default is 1000 msec")
    int sock_conn_timeout=1000;

    @Property(description="Max time in milliseconds to block on a read. 0 blocks forever")
    int sock_read_timeout=3000;

    @Property(description="Interval (ms) by which a disconnected stub attempts to reconnect to the GossipRouter")
    long reconnect_interval=10000L;
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    List<InetSocketAddress> initial_hosts=null; // (list of IpAddresses) hosts to be contacted for the initial membership
    final List<RouterStub>  stubs=new ArrayList<RouterStub>();
    Future<?> reconnect_future=null;
    protected volatile boolean running=true;

    public String getName() {
        return name;
    }
    
    public void init() throws Exception {
        super.init();
        if(initial_hosts == null || initial_hosts.isEmpty())
            throw new IllegalArgumentException("initial_hosts must contain the address of at least one GossipRouter");
        
        if(timeout <= sock_conn_timeout)
            throw new IllegalArgumentException("timeout (" + timeout + ") must be greater than sock_conn_timeout ("
                    + sock_conn_timeout + ")");
    }
    
    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=Util.parseCommaDelimetedHosts2(hosts,1);       
    }

    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
		super.stop();
        running=false;
		for (RouterStub stub : stubs) {
			try {
				stub.disconnect(group_addr, local_addr);
			} 
			catch (Exception e) {
			}
		}
        stopReconnector();
	}

    public void handleConnect() {
        if(group_addr == null || local_addr == null) {
            if(log.isErrorEnabled())
                log.error("group_addr or local_addr is null, cannot register with GossipRouter(s)");
        }
        else {
            if(log.isTraceEnabled())
                log.trace("registering " + local_addr + " under " + group_addr + " with GossipRouter");
             
            stubs.clear();
            
            for (InetSocketAddress host : initial_hosts) {
                RouterStub stub=new RouterStub(host.getHostName(), host.getPort(), null);
                stub.setConnectionListener(this);
				stubs.add(stub);
			}
            connect(group_addr, local_addr);
        }
    }


    public void handleDisconnect() {
    	for (RouterStub stub : stubs) {
			try {
				stub.disconnect(group_addr, local_addr);
			} 
			catch (Exception e) {
			}
		}
    }

    public void connectionStatusChange(RouterStub.ConnectionStatus state) {
        if(log.isDebugEnabled())
            log.debug("connection changed to " + state);
        if(state == RouterStub.ConnectionStatus.CONNECTED)
            stopReconnector();
        else
            startReconnector();
    }

    public void sendGetMembersRequest(String cluster_name, Promise promise) throws Exception{
        if(group_addr == null) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: group_addr is null, cannot get membership");            
            return;
        }
        
        final List<Address> initial_mbrs=new ArrayList<Address>();
        if(log.isTraceEnabled()) log.trace("fetching members from GossipRouter(s)");
        
        for(RouterStub stub : stubs) {
			try {
                List<PingData> rsps=stub.getMembers(group_addr);
                for(PingData rsp: rsps) {
                    Address logical_addr=rsp.getAddress();
                    initial_mbrs.add(logical_addr);

                    // 1. Set physical addresses
                    Collection<PhysicalAddress> physical_addrs=rsp.getPhysicalAddrs();
                    if(physical_addrs != null) {
                        for(PhysicalAddress physical_addr: physical_addrs)
                            down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address,PhysicalAddress>(logical_addr, physical_addr)));
                    }
                    
                    // 2. Set logical name
                    String logical_name=rsp.getLogicalName();
                    if(logical_name != null && logical_addr instanceof org.jgroups.util.UUID)
                        org.jgroups.util.UUID.add((org.jgroups.util.UUID)logical_addr, logical_name);
                }
			}
			catch (Exception e) {
			}
		}
        
        if(initial_mbrs.isEmpty()) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: found no members");
            return;
        }
        if(log.isTraceEnabled()) log.trace("consolidated mbrs from GossipRouter(s) are " + initial_mbrs);
        
        for(Address mbr_addr: initial_mbrs) {
            Message msg=new Message(mbr_addr);
            msg.setFlag(Message.OOB);
            PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name);
            msg.putHeader(name, hdr);
            if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + mbr_addr);
            down_prot.down(new Event(Event.MSG, msg));
        }
    }

    synchronized void startReconnector() {
        if(running && (reconnect_future == null || reconnect_future.isDone())) {
            if(log.isDebugEnabled())
                log.debug("starting reconnector");
            reconnect_future=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    connect(group_addr, local_addr);
                }
            }, 0, reconnect_interval, TimeUnit.MILLISECONDS);
        }
    }

    synchronized void stopReconnector() {
        if(reconnect_future != null) {
            reconnect_future.cancel(true);
            reconnect_future=null;
            if(log.isDebugEnabled())
                log.debug("stopping reconnector");
        }
    }

    protected void connect(String group, Address logical_addr) {
        String logical_name=org.jgroups.util.UUID.get(logical_addr);
        PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=physical_addr != null? new ArrayList<PhysicalAddress>() : null;
        if(physical_addr != null)
            physical_addrs.add(physical_addr);

        int num_faulty_conns=0;
        for (RouterStub stub : stubs) {
            try {
                if(log.isTraceEnabled())
                    log.trace("tring to connect to " + stub.getGossipRouterAddress());
                stub.connect(group, logical_addr, logical_name, physical_addrs);
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                log.error("failed connecting to " + stub.getGossipRouterAddress() + ": " +  e);
                num_faulty_conns++;
            }
        }
        if(num_faulty_conns == 0)
            stopReconnector();
    }
}

