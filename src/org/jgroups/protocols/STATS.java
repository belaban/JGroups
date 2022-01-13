package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides various stats
 * @author Bela Ban
 */
@MBean(description="Protocol which exposes various statistics such as sent messages, number of bytes received etc")
public class STATS extends Protocol {
    protected static final short   UP=1;
    protected static final short   DOWN=2;
    protected static final Address NULL_DEST=Global.NULL_ADDRESS;

    /** Global stats */
    @Component
    protected final MsgStats mstats=new MsgStats();

    /** Maintains stats per target destination */
    protected final ConcurrentMap<Address,MsgStats> sent=new ConcurrentHashMap<>();

    /** Maintains stats per receiver */
    protected final ConcurrentMap<Address,MsgStats> received=new ConcurrentHashMap<>();



    public void resetStats() {
        mstats.reset();
        sent.clear();
        received.clear();
    }



    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleViewChange(evt.getArg());
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        updateStats(msg.dest(), msg.src(), 1, msg.getLength(), UP);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        updateStats(batch.dest(), batch.sender(), batch.size(), batch.length(), UP);
        up_prot.up(batch);
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleViewChange(evt.getArg());
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        updateStats(msg.dest(), msg.src(), 1, msg.getLength(), DOWN);
        return down_prot.down(msg);
    }

    @ManagedOperation
    public String printStats() {
        Object key, val;
        StringBuilder sb=new StringBuilder();
        sb.append("sent:\n");
        for(Iterator<Map.Entry<Address,MsgStats>> it=sent.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Address,MsgStats> entry=it.next();
            key=entry.getKey();
            if(key == NULL_DEST) key="<mcast dest>";
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }
        sb.append("\nreceived:\n");
        for(Iterator<Map.Entry<Address,MsgStats>> it=received.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Address,MsgStats> entry=it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }

        return sb.toString();
    }

    private void handleViewChange(View view) {
        List<Address> members=view.getMembers();
        Set<Address> tmp=new LinkedHashSet<>(members);
        tmp.add(null); // for null destination (= mcast)
        sent.keySet().retainAll(tmp);
        received.keySet().retainAll(tmp);
    }

    protected void updateStats(Address dest, Address src, int num_msgs, int num_bytes, short direction) {
        boolean mcast=dest == null;

        if(direction == UP) { // received
            mstats.incrNumMsgsReceived(num_msgs);
            mstats.incrNumBytesReceived(num_bytes);
            if(mcast) {
                mstats.incrNumMcastMsgsReceived(num_msgs);
                mstats.incrNumMcastBytesReceived(num_bytes);
            }
            else {
                mstats.incrNumUcastMsgsReceived(num_msgs);
                mstats.incrNumUcastBytesReceived(num_bytes);
            }
        }
        else {                // sent
            mstats.incrNumMsgsSent(num_msgs);
            mstats.incrNumBytesSent(num_bytes);
            if(mcast) {
                mstats.incrNumMcastMsgsSent(num_msgs);
                mstats.incrNumMcastBytesSent(num_bytes);
            }
            else {
                mstats.incrNumUcastMsgsSent(num_msgs);
                mstats.incrNumUcastBytesSent(num_bytes);
            }
        }

        Address key=direction == UP? src : dest;
        if(key == null) key=NULL_DEST;
        Map<Address,MsgStats> map=direction == UP? received : sent;
        MsgStats entry=map.computeIfAbsent(key, k -> new MsgStats());
        if(direction == UP) {
            entry.incrNumMsgsSent(num_msgs);
            entry.incrNumBytesSent(num_bytes);
            if(mcast) {
                entry.incrNumMcastMsgsSent(num_msgs);
                entry.incrNumMcastBytesSent(num_bytes);
            }
            else {
                entry.incrNumUcastMsgsSent(num_msgs);
                entry.incrNumUcastBytesSent(num_bytes);
            }
        }
        else {
            entry.incrNumMsgsReceived(num_msgs);
            entry.incrNumBytesReceived(num_bytes);
            if(mcast) {
                entry.incrNumMcastMsgsReceived(num_msgs);
                entry.incrNumMcastBytesReceived(num_bytes);
            }
            else {
                entry.incrNumUcastMsgsReceived(num_msgs);
                entry.incrNumUcastBytesReceived(num_bytes);
            }
        }
    }


}
