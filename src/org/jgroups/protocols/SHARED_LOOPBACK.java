package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.stack.IpAddress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Loopback transport shared by all channels within the same VM. Property for testing is that no messages are lost. Allows
 * us to test various protocols (with ProtocolTester) at maximum speed.
 * @author Bela Ban
 */
public class SHARED_LOOPBACK extends TP {
    private static int next_port=10000;

    private PhysicalAddress physical_addr=null;
    private Address local_addr=null;

    /** Map of cluster names and address-protocol mappings. Used for routing messages to all or single members */
    private static final Map<String,Map<Address,SHARED_LOOPBACK>> routing_table=new ConcurrentHashMap<String,Map<Address,SHARED_LOOPBACK>>();


    public SHARED_LOOPBACK() {
    }

    public boolean supportsMulticasting() {
        return false;
    }

    public String toString() {
        return "SHARED_LOOPBACK(local address: " + local_addr + ')';
    }

    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(channel_name);
        if(dests == null) {
            if(log.isWarnEnabled())
                log.warn("no destination found for " + channel_name);
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
            if(log.isWarnEnabled())
                log.warn("no destination found for " + channel_name);
            return;
        }
        SHARED_LOOPBACK target=dests.get(dest);
        if(target == null) {
            if(log.isWarnEnabled())
                log.warn("destination address " + dest + " not found");
            return;
        }
        target.receive(local_addr, data, offset, length);
    }

    protected void sendToSingleMember(Address dest, byte[] buf, int offset, int length) throws Exception {
        Map<Address,SHARED_LOOPBACK> dests=routing_table.get(channel_name);
        if(dests == null) {
            if(log.isWarnEnabled())
                log.warn("no destination found for " + channel_name);
            return;
        }
        SHARED_LOOPBACK target=dests.get(dest);
        if(target == null) {
            if(log.isWarnEnabled())
                log.warn("destination address " + dest + " not found");
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

    public void init() throws Exception {
        local_addr=new IpAddress("127.0.0.1", next_port++);
        super.init();
                        
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        super.stop();
    }


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

            case Event.DISCONNECT:
                unregister(channel_name, local_addr);
                break;
        }

        return retval;
    }

    private static void register(String channel_name, Address local_addr, SHARED_LOOPBACK shared_loopback) {
        Map<Address,SHARED_LOOPBACK> map=routing_table.get(channel_name);
        if(map == null) {
            map=new ConcurrentHashMap<Address,SHARED_LOOPBACK>();
            routing_table.put(channel_name, map);
        }
        map.put(local_addr, shared_loopback);
    }

    private static void unregister(String channel_name, Address local_addr) {
        Map<Address,SHARED_LOOPBACK> map=routing_table.get(channel_name);
        if(map != null) {
            map.remove(local_addr);
            if(map.isEmpty()) {
                routing_table.remove(channel_name);
            }
        }
    }

}
