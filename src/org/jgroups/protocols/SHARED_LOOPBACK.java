package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.AsciiString;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Loopback transport shared by all channels within the same VM. Property for testing is that no messages are lost. Allows
 * us to test various protocols (with ProtocolTester) at maximum speed.
 * @author Bela Ban
 */
public class SHARED_LOOPBACK extends TP {
    protected PhysicalAddress  physical_addr;

    @ManagedAttribute(description="The current view",writable=false)
    protected volatile View    curr_view;

    protected volatile boolean is_server=false, is_coord=false;

    /** Map of cluster names and address-protocol mappings. Used for routing messages to all or single members */
    private static final ConcurrentMap<AsciiString,Map<Address,SHARED_LOOPBACK>> routing_table=new ConcurrentHashMap<>();


    public boolean supportsMulticasting() {
        return true; // kind of...
    }

    public View    getView()  {return curr_view;}
    public boolean isServer() {return is_server;}
    public boolean isCoord()  {return is_coord;}

    public String toString() {
        return "SHARED_LOOPBACK(local address: " + local_addr + ')';
    }

    @ManagedOperation(description="Dumps the contents of the routing table")
    public static String dumpRoutingTable() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<AsciiString,Map<Address,SHARED_LOOPBACK>> entry: routing_table.entrySet()) {
            AsciiString cluster_name=entry.getKey();
            Set<Address> mbrs=entry.getValue().keySet();
            sb.append(cluster_name).append(": ").append(mbrs).append("\n");
        }
        return sb.toString();
    }


    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(this.cluster_name);
        if(dests == null) {
            log.trace("no destination found for " + this.cluster_name);
            return;
        }
        for(Map.Entry<Address,SHARED_LOOPBACK> entry: dests.entrySet()) {
            Address dest=entry.getKey();
            SHARED_LOOPBACK target=entry.getValue();
            if(Objects.equals(local_addr, dest))
                continue; // message was already looped back
            try {
                target.receive(local_addr, data, offset, length);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedSendingMessageTo") + dest, t);
            }
        }
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        sendToSingleMember(dest, data, offset, length);
    }

    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(cluster_name);
        if(dests == null) {
            log.trace("no destination found for " + cluster_name);
            return;
        }
        SHARED_LOOPBACK target=dests.get(dest);
        if(target == null) {
            log.trace("destination address " + dest + " not found");
            return;
        }
        target.receive(local_addr, buf, offset, length);
    }


    public static List<PingData> getDiscoveryResponsesFor(String cluster_name) {
        if(cluster_name == null)
            return null;
        Map<Address,SHARED_LOOPBACK> mbrs=routing_table.get(new AsciiString(cluster_name));
        List<PingData> rsps=new ArrayList<>(mbrs != null? mbrs.size() : 0);
        if(mbrs != null) {
            for(Map.Entry<Address,SHARED_LOOPBACK> entry: mbrs.entrySet()) {
                Address addr=entry.getKey();
                SHARED_LOOPBACK slp=entry.getValue();
                PingData data=new PingData(addr, slp.isServer(), NameCache.get(addr), null).coord(slp.isCoord());
                rsps.add(data);
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

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.BECOME_SERVER: // called after client has joined and is fully working group member
                is_server=true;
                break;
            case Event.VIEW_CHANGE:
            case Event.TMP_VIEW:
                curr_view=evt.getArg();
                Address[] mbrs=((View)evt.getArg()).getMembersRaw();
                is_coord=local_addr != null && mbrs != null && mbrs.length > 0 && local_addr.equals(mbrs[0]);
                break;
            case Event.GET_PING_DATA:
                return getDiscoveryResponsesFor(evt.getArg()); // don't pass further down
        }

        return retval;
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

    protected static void register(AsciiString channel_name, Address local_addr, SHARED_LOOPBACK shared_loopback) {
        Map<Address,SHARED_LOOPBACK> map=routing_table.get(channel_name);
        if(map == null) {
            map=new ConcurrentHashMap<>();
            Map<Address,SHARED_LOOPBACK> tmp=routing_table.putIfAbsent(channel_name,map);
            if(tmp != null)
                map=tmp;
        }
        map.put(local_addr, shared_loopback);
    }

    protected static void unregister(AsciiString channel_name, Address local_addr) {
        Map<Address,SHARED_LOOPBACK> map=channel_name != null? routing_table.get(channel_name) : null;
        if(map != null) {
            map.remove(local_addr);
            if(map.isEmpty())
                routing_table.remove(channel_name);
        }
    }

}
