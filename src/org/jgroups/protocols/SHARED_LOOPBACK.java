package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.AsciiString;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Loopback transport shared by all channels within the same VM. Property for testing is that no messages are lost.
 * Allows us to test various protocols at maximum speed.
 * @author Bela Ban
 */
public class SHARED_LOOPBACK extends TP {
    protected short port=1;
    protected PhysicalAddress physical_addr;

    @ManagedAttribute(description="The current view")
    protected volatile View curr_view;

    protected volatile boolean is_server=false, is_coord=false;

    /**
     * Map of cluster names and address-protocol mappings. Used for routing messages to all or single members
     */
    protected static final Map<AsciiString,Map<Address,SHARED_LOOPBACK>>      routing_table=new HashMap<>();
    protected static final Function<AsciiString,Map<Address,SHARED_LOOPBACK>> FUNC=n -> new HashMap<>();

    public boolean supportsMulticasting()   {return true;} // kind of...
    public View    getView()                {return curr_view;}
    public boolean isServer()               {return is_server;}
    public boolean isCoord()                {return is_coord;}
    public SHARED_LOOPBACK coord(boolean b) {this.is_coord=is_server=b; return this;}
    public String  toString()               {return "SHARED_LOOPBACK(local address: " + local_addr + ')';}

    @ManagedOperation(description="Dumps the contents of the routing table")
    public static String dumpRoutingTable() {
        StringBuilder sb=new StringBuilder();
        synchronized(routing_table) {
            for(Map.Entry<AsciiString,Map<Address,SHARED_LOOPBACK>> entry : routing_table.entrySet()) {
                AsciiString cluster_name=entry.getKey();
                Set<Address> mbrs=entry.getValue().keySet();
                sb.append(cluster_name).append(": ").append(mbrs).append("\n");
            }
        }
        return sb.toString();
    }


    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        List<SHARED_LOOPBACK> targets;
        synchronized(routing_table) {
            Map<Address,SHARED_LOOPBACK> dests=routing_table.get(this.cluster_name);
            if(dests == null) {
                log.trace("no destination found for " + this.cluster_name);
                return;
            }
            targets=dests.entrySet().stream().filter(e -> !Objects.equals(local_addr, e.getKey()))
              .map(Map.Entry::getValue).collect(Collectors.toList());
        }

        targets.forEach(target -> {
            try {
                target.receive(local_addr, data, offset, length);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedSendingMessageTo") + target.localAddress(), t);
            }
        });
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        sendToSingleMember(dest, data, offset, length);
    }

    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        SHARED_LOOPBACK target;
        synchronized(routing_table) {
            Map<Address,SHARED_LOOPBACK> dests=routing_table.get(cluster_name);
            if(dests == null) {
                log.trace("no destination found for " + cluster_name);
                return;
            }
            target=dests.get(dest);
            if(target == null) {
                log.trace("%s: destination address %s not found, routing table:\n%s\n", local_addr, dest, dumpRoutingTable());
                return;
            }
        }
        target.receive(local_addr, buf, offset, length);
    }


    public static List<PingData> getDiscoveryResponsesFor(String cluster_name) {
        if(cluster_name == null)
            return null;
        List<PingData> rsps=new ArrayList<>();
        synchronized(routing_table) {
            Map<Address,SHARED_LOOPBACK> mbrs=routing_table.get(new AsciiString(cluster_name));
            if(mbrs != null) {
                for(Map.Entry<Address,SHARED_LOOPBACK> entry: mbrs.entrySet()) {
                    Address addr=entry.getKey();
                    SHARED_LOOPBACK l=entry.getValue();
                    PingData data=new PingData(addr, l.isServer(), NameCache.get(addr), null).coord(l.isCoord());
                    rsps.add(data);
                }
            }
        }
        return rsps;
    }

    public String getInfo() {
        return toString();
    }


    protected PhysicalAddress getPhysicalAddress() {
        return physical_addr;
    }

    /*------------------------------ Protocol interface ------------------------------ */


    public Object down(Event evt) {
        Object retval=super.down(evt);

        switch(evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                register(cluster_name, local_addr, this);
                break;
            case Event.DISCONNECT:
                unregister(cluster_name, local_addr);
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                is_server=true;
                break;
            case Event.VIEW_CHANGE:
            case Event.TMP_VIEW:
                handleViewChange(evt.getArg());
                break;
            case Event.GET_PING_DATA:
                return getDiscoveryResponsesFor(evt.getArg()); // don't pass further down
            case Event.GET_PHYSICAL_ADDRESS:
                if(cluster_name == null)
                    return retval;
                Address mbr=evt.getArg();
                synchronized(routing_table) {
                    Map<Address,SHARED_LOOPBACK> map=routing_table.get(cluster_name);
                    SHARED_LOOPBACK lp=map != null? map.get(mbr) : null;
                    return lp != null? lp.getPhysicalAddress() : null;
                }
        }

        return retval;
    }

    public void init() throws Exception {
        super.init();
        physical_addr=new IpAddress(InetAddress.getLoopbackAddress(), port++);
    }

    public void stop() {
        super.stop();
        is_server=is_coord=false;
        unregister(cluster_name, local_addr);
    }

    public void destroy() {
        super.destroy();
        // We cannot clear the routing table, as it is shared between channels, and so we would clear the routing for
        // a different channel, too !
        unregister(cluster_name, local_addr);
    }

    protected void handleViewChange(View v) {
        curr_view=v;
        is_coord=Objects.equals(local_addr, v.getCoord());
    }

    protected static void register(AsciiString cluster, Address local_addr, SHARED_LOOPBACK shared_loopback) {
        synchronized(routing_table) {
            Map<Address,SHARED_LOOPBACK> map=routing_table.computeIfAbsent(cluster, FUNC);
            if(map.isEmpty()) {
                // the first member will become coord (may be changed by view changes/merges later)
                // https://issues.jboss.org/browse/JGRP-2395
                shared_loopback.coord(true);
            }
            map.putIfAbsent(local_addr, shared_loopback);
        }
    }

    protected static void unregister(AsciiString cluster, Address local_addr) {
        synchronized(routing_table) {
            Map<Address,SHARED_LOOPBACK> map=cluster != null? routing_table.get(cluster) : null;
            if(map != null && map.remove(local_addr) != null && map.isEmpty())
                routing_table.remove(cluster);
        }
    }

}
