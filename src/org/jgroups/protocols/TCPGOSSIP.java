
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Promise;
import org.jgroups.util.Tuple;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


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
 * @version $Id: TCPGOSSIP.java,v 1.49 2009/12/22 09:02:01 belaban Exp $
 */
@DeprecatedProperty(names={"gossip_refresh_rate"})
public class TCPGOSSIP extends Discovery implements RouterStub.ConnectionListener {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Max time for socket creation. Default is 1000 msec")
    int sock_conn_timeout=1000;

    @Property(description="Max time in milliseconds to block on a read. 0 blocks forever")
    int sock_read_timeout=3000;

    @Property(description="Interval (ms) by which a disconnected stub attempts to reconnect to the GossipRouter")
    long reconnect_interval=10000L;
    
    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership", 
    		converter=PropertyConverters.InitialHosts2.class)
    public void setInitialHosts(List<InetSocketAddress> initial_hosts) {
        if(initial_hosts == null || initial_hosts.isEmpty())
            throw new IllegalArgumentException("initial_hosts must contain the address of at least one GossipRouter");

    	this.initial_hosts = initial_hosts ;
    }

    public List<InetSocketAddress> getInitialHosts() {
        return initial_hosts;
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    List<InetSocketAddress> initial_hosts=null; // (list of IpAddresses) hosts to be contacted for the initial membership
    final List<RouterStub>  stubs=new ArrayList<RouterStub>();
    Future<?> reconnect_future=null;
    Future<?> connection_checker=null;
    protected volatile boolean running=true;

    public void init() throws Exception {
        super.init();
        
        if(timeout <= sock_conn_timeout)
            throw new IllegalArgumentException("timeout (" + timeout + ") must be greater than sock_conn_timeout ("
                    + sock_conn_timeout + ")");

        // we cannot use TCPGOSSIP together with TUNNEL (https://jira.jboss.org/jira/browse/JGRP-1101)
        TP transport=getTransport();
        if(transport instanceof TUNNEL)
            throw new IllegalStateException("TCPGOSSIP cannot be used with TUNNEL; use either TUNNEL:PING or " +
                    "TCP:TCPGOSSIP as valid configurations");
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

    public void destroy() {
        for(RouterStub stub : stubs) {
            stub.destroy();
        }
        super.destroy();
    }

    public void handleConnect() {
        if(group_addr == null || local_addr == null) {
            if(log.isErrorEnabled())
                log.error("group_addr or local_addr is null, cannot register with GossipRouter(s)");
        }
        else {
            if(log.isTraceEnabled())
                log.trace("registering " + local_addr + " under " + group_addr + " with GossipRouter");
            for(RouterStub stub: stubs) // if there are any stubs, destroy them
                stub.destroy();
            stubs.clear();
            
            for (InetSocketAddress host : initial_hosts) {
                RouterStub stub=new RouterStub(host.getHostName(), host.getPort(), null);
                stub.setConnectionListener(this);
				stubs.add(stub);
			}
            connect(group_addr, local_addr);
            startConnectionChecker();
        }
    }


    public void handleDisconnect() {
    	for (RouterStub stub : stubs) {
			try {
				stub.disconnect(group_addr, local_addr);
                stub.destroy();
			} 
			catch (Exception e) {
			}
		}
        stopConnectionChecker();
    }

    public void connectionStatusChange(RouterStub.ConnectionStatus state) {
        if(log.isDebugEnabled())
            log.debug("connection changed to " + state);
        if(state == RouterStub.ConnectionStatus.CONNECTED)
            stopReconnector();
        else
            startReconnector();
    }

    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception{
        if(group_addr == null) {
            if(log.isErrorEnabled()) log.error("cluster_name is null, cannot get membership");            
            return;
        }
        
        if(log.isTraceEnabled()) log.trace("fetching members from GossipRouter(s)");

        final List<PingData> responses=new LinkedList<PingData>();
        for(RouterStub stub : stubs) {
            try {
                List<PingData> rsps=stub.getMembers(group_addr);
                responses.addAll(rsps);
            }
            catch(Throwable e) {
			}
		}

        final Set<Address> initial_mbrs=new HashSet<Address>();
        for(PingData rsp: responses) {
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
        
        if(initial_mbrs.isEmpty()) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: found no members");
            return;
        }
        if(log.isTraceEnabled()) log.trace("consolidated mbrs from GossipRouter(s) are " + initial_mbrs);
        
        for(Address mbr_addr: initial_mbrs) {
            Message msg=new Message(mbr_addr);
            msg.setFlag(Message.OOB);
            PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name);
            hdr.return_view_only=return_views_only;
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
            reconnect_future.cancel(false);
            reconnect_future=null;
            if(log.isDebugEnabled())
                log.debug("stopping reconnector");
        }
    }

    synchronized void startConnectionChecker() {
        if(running && (connection_checker == null || connection_checker.isDone())) {
            if(log.isDebugEnabled())
                log.debug("starting connection checker");
            connection_checker=timer.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    for(RouterStub stub: stubs) {
                        stub.checkConnection();
                    }
                }
            }, 0, reconnect_interval, TimeUnit.MILLISECONDS);
        }
    }

    synchronized void stopConnectionChecker() {
        if(connection_checker != null) {
            connection_checker.cancel(false);
            connection_checker=null;
            if(log.isDebugEnabled())
                log.debug("stopping connection checker");
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
                    log.trace("trying to connect to " + stub.getGossipRouterAddress());
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

