package org.jgroups.fork;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Protocol stack which maintains the mapping between fork-channel IDs and ForkChannels
 * @author Bela Ban
 * @since  3.4
 */
public class ForkProtocolStack extends ProtocolStack {
    protected Address local_addr;
    protected final ConcurrentMap<String,JChannel> fork_channels=new ConcurrentHashMap<String,JChannel>();

    public JChannel get(String fork_channel_id)                                {return fork_channels.get(fork_channel_id);}
    public JChannel putIfAbsent(String fork_channel_id, JChannel fork_channel) {return fork_channels.putIfAbsent(fork_channel_id, fork_channel);}
    public void     remove(String fork_channel_id)                             {fork_channels.remove(fork_channel_id);}

    public Object down(Event evt) {
        return down_prot.down(evt);
    }

    public void setLocalAddress(Address addr) {
        if(local_addr != null && addr != null && local_addr.equals(addr))
            return;
        this.local_addr=addr;
        down_prot.down(new Event(Event.SET_LOCAL_ADDRESS, addr));
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
                if(hdr == null)
                    break;
                if(hdr.getForkChannelId() == null)
                    throw new IllegalArgumentException("header has a null fork_channel_id");
                JChannel fork_channel=get(hdr.getForkChannelId());
                return fork_channel.up(evt);

            case Event.VIEW_CHANGE:
                for(JChannel ch: fork_channels.values())
                    ch.up(evt);
                break;
        }
        return null;
    }

    public void up(MessageBatch batch) {
        // Sort fork messages by fork-channel-id
        Map<String,List<Message>> map=new HashMap<String,List<Message>>();
        for(Message msg: batch) {
            FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
            if(hdr != null) {
                batch.remove(msg);
                List<Message> list=map.get(hdr.getForkChannelId());
                if(list == null) {
                    list=new ArrayList<Message>();
                    map.put(hdr.getForkChannelId(), list);
                }
                list.add(msg);
            }
        }

        // Now pass fork messages up, batched by fork-channel-id
        for(Map.Entry<String,List<Message>> entry: map.entrySet()) {
            String fork_channel_id=entry.getKey();
            List<Message> list=entry.getValue();
            JChannel fork_channel=get(fork_channel_id);
            if(fork_channel == null) {
                log.warn("fork-channel for id=%s not found; discarding message", fork_channel_id);
                continue;
            }
            MessageBatch mb=new MessageBatch(batch.dest(), batch.sender(), batch.clusterName(), batch.multicast(), list);
            try {
                fork_channel.up(mb);
            }
            catch(Throwable t) {
                log.error("failed passing up batch", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }
}
