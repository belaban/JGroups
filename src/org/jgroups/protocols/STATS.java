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
    protected static final Address NULL_DEST=Global.NULL_ADDRESS;

    /** Global stats */
    @Component
    protected final MsgStats                        mstats=new MsgStats();

    /** Maintains stats per target destination */
    protected final ConcurrentMap<Address,MsgStats> sent=new ConcurrentHashMap<>();

    /** Maintains stats per receiver */
    protected final ConcurrentMap<Address,MsgStats> received=new ConcurrentHashMap<>();



    public void resetStats() {
        mstats.reset();
        sent.clear();
        received.clear();
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleViewChange(evt.getArg());
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        sent(msg);
        return down_prot.down(msg);
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleViewChange(evt.getArg());
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        received(msg);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        received(batch);
        up_prot.up(batch);
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

    protected void handleViewChange(View view) {
        List<Address> members=view.getMembers();
        Set<Address> tmp=new LinkedHashSet<>(members);
        tmp.add(null); // for null destination (= mcast)
        sent.keySet().retainAll(tmp);
        received.keySet().retainAll(tmp);
    }


    protected void sent(Message msg) {
        mstats.sent(msg);

        Address key=msg.dest();
        if(key == null) key=NULL_DEST;
        MsgStats entry=((Map<Address,MsgStats>)sent).computeIfAbsent(key, k -> new MsgStats());
        entry.sent(msg);
    }

    protected void received(Message msg) {
        mstats.received(msg);

        Address key=msg.src();
        if(key == null) key=NULL_DEST;
        MsgStats entry=((Map<Address,MsgStats>)received).computeIfAbsent(key, k -> new MsgStats());
        entry.received(msg);
    }

    protected void received(MessageBatch batch) {
        mstats.received(batch);

        Address key=batch.sender();
        if(key == null) key=NULL_DEST;
        MsgStats entry=((Map<Address,MsgStats>)received).computeIfAbsent(key, k -> new MsgStats());
        entry.received(batch);
    }


}
