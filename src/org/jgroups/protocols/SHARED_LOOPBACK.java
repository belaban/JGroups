package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedOperation;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Loopback transport shared by all channels within the same VM. Property for testing is that no messages are lost. Allows
 * us to test various protocols (with ProtocolTester) at maximum speed.
 * @author Bela Ban
 */
public class SHARED_LOOPBACK extends TP {
    private PhysicalAddress physical_addr=null;

    /** Map of cluster names and address-protocol mappings. Used for routing messages to all or single members */
    private static final ConcurrentMap<String,Map<Address,SHARED_LOOPBACK>> routing_table=new ConcurrentHashMap<String,Map<Address,SHARED_LOOPBACK>>();


    public boolean supportsMulticasting() {
        return true; // kind of...
    }

    public String toString() {
        return "SHARED_LOOPBACK(local address: " + local_addr + ')';
    }

    @ManagedOperation(description="Dumps the contents of the routing table")
    public static String dumpRoutingTable() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,Map<Address,SHARED_LOOPBACK>> entry: routing_table.entrySet()) {
            String cluster_name=entry.getKey();
            Set<Address> mbrs=entry.getValue().keySet();
            sb.append(cluster_name).append(": ").append(mbrs).append("\n");
        }
        return sb.toString();
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(channel_name);
        if(dests == null) {
            if(log.isTraceEnabled())
                log.trace("no destination found for " + channel_name);
            return;
        }
        for(Map.Entry<Address,SHARED_LOOPBACK> entry: dests.entrySet()) {
            Address dest=entry.getKey();
            SHARED_LOOPBACK target=entry.getValue();
            try {
                target.receive(local_addr, data, offset, length);
            }
            catch(Throwable t) {
                log.error("failed sending message to " + dest, t);
            }
        }
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(channel_name);
        if(dests == null) {
            if(log.isTraceEnabled())
                log.trace("no destination found for " + channel_name);
            return;
        }
        SHARED_LOOPBACK target=dests.get(dest);
        if(target == null) {
            if(log.isTraceEnabled())
                log.trace("destination address " + dest + " not found");
            return;
        }
        target.receive(local_addr, data, offset, length);
    }

    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(channel_name);
        if(dests == null) {
            if(log.isTraceEnabled())
                log.trace("no destination found for " + channel_name);
            return;
        }
        SHARED_LOOPBACK target=dests.get(dest);
        if(target == null) {
            if(log.isTraceEnabled())
                log.trace("destination address " + dest + " not found");
            return;
        }
        target.receive(local_addr, buf, offset, length);
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
                register(channel_name, local_addr, this);
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return retval;
    }


    public void destroy() {
        super.destroy();
        // We cannot clear the routing table, as it is shared between channels, and so we would clear the routing for
        // a different channel, too !
        unregister(channel_name, local_addr);
    }

    protected static void register(String channel_name, Address local_addr, SHARED_LOOPBACK shared_loopback) {
        Map<Address,SHARED_LOOPBACK> map=routing_table.get(channel_name);
        if(map == null) {
            map=new ConcurrentHashMap<Address,SHARED_LOOPBACK>();
            Map<Address,SHARED_LOOPBACK> tmp=routing_table.putIfAbsent(channel_name,map);
            if(tmp != null)
                map=tmp;
        }
        map.put(local_addr, shared_loopback);
    }

    protected static void unregister(String channel_name, Address local_addr) {
        Map<Address,SHARED_LOOPBACK> map=channel_name != null? routing_table.get(channel_name) : null;
        if(map != null) {
            map.remove(local_addr);
            if(map.isEmpty()) {
                routing_table.remove(channel_name);
            }
        }
    }

}
