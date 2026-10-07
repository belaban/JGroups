package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Maintains a version number and adds it to all outbound messages. Inbound messages received with a different version
 * are dropped.
 * Mainly used by an in-place configuration change (https://redhat.atlassian.net/browse/JGRP-3043)
 * @author Bela Ban
 * @since  5.6.0
 */
public class VERSION extends Protocol {
    @Property(description="The current version number")
    protected int             version;

    @ManagedAttribute(description="Number of messages dropped on reception due to non-matching version number")
    protected final LongAdder dropped=new LongAdder();

    public int     version()            {return version;}
    public VERSION version(int version) {this.version=version; return this;}
    public long    dropped()            {return dropped.sum();}


    @Override
    public void resetStats() {
        dropped.reset();
    }

    @Override
    public Object down(Message msg) {
        msg.putHeader(this.id, new VersionHeader(this.version));
        return down_prot.down(msg);
    }

    @Override
    public CompletableFuture<Object> down(Message msg, boolean async) {
        msg.putHeader(this.id, new VersionHeader(this.version));
        return down_prot.down(msg, async);
    }

    @Override
    public Object up(Message msg) {
        VersionHeader hdr=msg.getHeader(this.id);
        if(hdr != null && version != hdr.ver) {
            dropped.increment();
            drop(hdr.ver, msg);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            VersionHeader hdr=msg.getHeader(this.id);
            if(hdr != null && version != hdr.ver) {
                dropped.increment();
                drop(hdr.ver, msg);
                it.remove();
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void drop(int other_version, Message msg) {
        log.trace("%s: dropped message from %s due to version mismatch (my version: %d, received version: %d) hdrs: %s",
                  local_addr, msg.src(), version, other_version, msg.printHeaders());
    }



    public static class VersionHeader extends Header {
        protected int ver;

        public VersionHeader() {
        }

        public VersionHeader(int ver) {
            this.ver=ver;
        }

        @Override
        public short getMagicId() {return 102;}

        @Override
        public Supplier<? extends Header> create() {return VersionHeader::new;}

        @Override
        public int serializedSize() {
            return Integer.BYTES;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(ver);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            ver=in.readInt();
        }

        @Override
        public String toString() {
            return String.format("version=%d", ver);
        }
    }
}
