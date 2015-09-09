package org.jgroups.fork;

import org.jgroups.*;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.Protocol;
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
    protected       Address                        local_addr;
    protected final String                         fork_stack_id;
    protected final ConcurrentMap<String,JChannel> fork_channels=new ConcurrentHashMap<>();
    protected final UnknownForkHandler             unknownForkHandler;
    protected final List<Protocol>                 protocols;

    // init() increments and destroy() decrements
    // 1 -> 0: destroy the stack and remove it from FORK. Calls destroy() in all fork stack protocols before
    protected int                                  inits;

    // connect() increments and disconnect decrements.
    // 0 -> 1: connect the stack (calls start() in all protocols of the fork stack)
    // 1 -> 0: disconnect the stack (calls stop() in all protocols of the fork stack)
    protected int                                  connects;


    public ForkProtocolStack(UnknownForkHandler unknownForkHandler, List<Protocol> protocols, String fork_stack_id) {
        this.unknownForkHandler = unknownForkHandler;
        this.fork_stack_id=fork_stack_id;
        this.protocols=new ArrayList<>(protocols != null? protocols.size() : 0);
        if(protocols != null)
            for(int i=protocols.size()-1; i >= 0; i--)
                this.protocols.add(protocols.get(i));
    }

    public ConcurrentMap<String,JChannel> getForkChannels()                   {return fork_channels;}
    public JChannel                       get(String fork_channel_id)         {return fork_channels.get(fork_channel_id);}
    public JChannel                       putIfAbsent(String id, JChannel fc) {return fork_channels.putIfAbsent(id, fc);}
    public void                           remove(String fork_channel_id)      {fork_channels.remove(fork_channel_id);}
    public synchronized int               getInits()                          {return inits;}
    public synchronized int               getConnects()                       {return connects;}

    public Object down(Event evt) {
        return down_prot.down(evt);
    }

    public void setLocalAddress(Address addr) {
        if(local_addr != null && addr != null && local_addr.equals(addr))
            return;
        this.local_addr=addr;
        down_prot.down(new Event(Event.SET_LOCAL_ADDRESS, addr));
    }

    @Override
    public List<Protocol> getProtocols() {
        return new ArrayList<>(protocols); // copy because Collections.reverse() will be called on the return value
    }

    public synchronized ForkProtocolStack incrInits() {
        ++inits;
        return this;
    }


    @Override
    public void init() throws Exception {
        super.init();
    }

    @Override
    public synchronized void startStack(String cluster, Address local_addr) throws Exception {
        if(++connects == 1)
            super.startStack(cluster, local_addr);
    }

    @Override
    public synchronized void stopStack(String cluster) {
        if(--connects == 0)
            super.stopStack(cluster);
    }

    @Override
    public synchronized void destroy() {
        if(--inits == 0) {
            super.destroy();
            this.protocols.clear();
            FORK fork=(FORK)findProtocol(FORK.class);
            fork.remove(this.fork_stack_id);
        }
    }



    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
                if(hdr == null)
                    break;
                String forkId = hdr.getForkChannelId();
                if(forkId == null)
                    throw new IllegalArgumentException("header has a null fork_channel_id");
                JChannel fork_channel=get(forkId);
                if (fork_channel == null) {
                    return this.unknownForkHandler.handleUnknownForkChannel(msg, forkId);
                }
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
        Map<String,List<Message>> map=new HashMap<>();
        for(Message msg: batch) {
            FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
            if(hdr != null) {
                batch.remove(msg);
                List<Message> list=map.get(hdr.getForkChannelId());
                if(list == null) {
                    list=new ArrayList<>();
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
                log.debug("fork-channel for id=%s not found; discarding message", fork_channel_id);
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
