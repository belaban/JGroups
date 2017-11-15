package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Maintains mappings between addresses and logical names.
 * <p>
 * JIRA: https://issues.jboss.org/browse/JGRP-2104
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Maintains mappings of addresses and their logical names")
public class NAMING extends Protocol {
    protected Address       local_addr;
    protected volatile View view;

    @Property(description="Stagger timeout (in ms). Staggering will be a random timeout in range [0 .. stagger_timeout]")
    protected long stagger_timeout=500;

    public Object down(Event evt) {
        handleEvent(evt);
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        handleEvent(evt);
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        Header hdr=msg.getHeader(id);
        if(hdr != null)
            return handleMessage(msg, hdr);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            Header hdr=msg.getHeader(id);
            if(hdr != null)
                handleMessage(msg, hdr);
        }
        batch.remove(msg -> msg.getHeader(id) != null); // remove all messages with NAMING headers
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected Object handleMessage(Message msg, Header hdr) {
        switch(hdr.type) {
            case CACHE_REQ:
                handleCacheRequest(msg.getSrc());
                break;
            case CACHE_RSP:
                handleCacheResponse(msg);
                break;
        }
        return null;
    }

    /** Typically received by the coord, which sends its cache contents to the sender (new joiner). However, we don't
     * send one large message, but rather N messages (1 per cluster member). The reason is that we don't know where in
     * the stack NAMING will be running and therefore cannot assume fragmentation of large messages.
     */
    protected void handleCacheRequest(Address sender) {
        int view_size=view != null? view.size() : 0;
        if(view_size == 0)
            return;
        for(Address addr: view.getMembersRaw()) {
            if(Objects.equals(addr, sender))
                continue;
            String logical_name=NameCache.get(addr);
            if(logical_name == null)
                continue;
            Header hdr=new Header(Type.CACHE_RSP, addr, logical_name);
            Message msg=new EmptyMessage(sender).putHeader(id, hdr);
            if(log.isTraceEnabled())
                log.trace("%s: sending %s to %s", local_addr, hdr, sender);
            try {
                down_prot.down(msg);
            }
            catch(Throwable t) {
                log.error("failed sending CACHE_RSP", t);
            }
        }
    }

    protected void handleCacheResponse(Message msg) {
        Header hdr=msg.getHeader(id);
        if(hdr != null && hdr.addr != null && hdr.name != null) {
            if(log.isTraceEnabled())
                log.trace("%s: received %s from %s", local_addr, hdr, msg.getSrc());
            NameCache.add(hdr.addr, hdr.name);
        }
    }


    protected void handleEvent(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View old_view=view, new_view=evt.getArg();
                this.view=new_view;
                if(old_view == null) { // first join
                    Util.sleepRandom(0, stagger_timeout);

                    // 1. send my own mapping to all
                    multicastOwnMapping();

                    // 2. ask the coordinator to send us the cache contents
                    Address coord=new_view.getCoord();
                    if(Objects.equals(local_addr, coord))
                        return;
                    Message msg=new EmptyMessage(coord).setFlag(Message.Flag.OOB).putHeader(id, new Header(Type.CACHE_REQ));
                    down_prot.down(msg);
                    return;
                }
                if(new_view instanceof MergeView) {
                    Util.sleepRandom(0, stagger_timeout);
                    multicastOwnMapping();
                }
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
    }

    protected void multicastOwnMapping() {
        String logical_name=NameCache.get(local_addr);
        if(logical_name != null) {
            Message msg=new EmptyMessage(null).setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.DONT_LOOPBACK)
              .putHeader(id, new Header(Type.CACHE_RSP, local_addr, logical_name));
            down_prot.down(msg);
        }
    }


    public enum Type {
        CACHE_REQ, // req to the coord, which returns N CACHE_RSPs where N = cluster size
        CACHE_RSP  // contains address and logical_name
    }


    public static class Header extends org.jgroups.Header {
        protected Type    type;
        protected Address addr;
        protected String  name;


        public Header() {
        }

        public Header(Type t) {this.type=t;}
        public Header(Type t, Address addr, String name) {
            this(t);
            this.addr=addr;
            this.name=name;
        }

        public Supplier<? extends org.jgroups.Header> create()     {return Header::new;}
        public short                                  getMagicId() {return 89;}


        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeShort(type.ordinal());
            Util.writeAddress(addr, out);
            Bits.writeString(name, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=Type.values()[in.readShort()];
            addr=Util.readAddress(in);
            name=Bits.readString(in);
        }

        @Override
        public String toString() {
            return String.format("%s addr=%s name=%s", type, addr, name);
        }

        @Override
        public int serializedSize() {
            return Global.SHORT_SIZE + Util.size(addr) + Util.size(name);
        }
    }
}
