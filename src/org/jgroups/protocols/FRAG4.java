package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.FragmentedMessage;
import org.jgroups.Message;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Range;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Objects;


/**
 * Fragmentation layer. Fragments messages larger than frag_size into smaller packets. Reassembles fragmented packets
 * into bigger ones. The fragmentation ID is added to the messages as a header (and removed at the receiving side).
 * <br/>
 * Each fragment is identified by (a) the sender (part of the message to which the header is appended),
 * (b) the fragmentation ID (which is unique (monotonically increasing) and (c) the fragement ID which ranges from 0
 * to number_of_fragments-1.
 * <br/>
 * Requirement: lossless delivery (e.g. NAKACK2, UNICAST3).
 * No requirement on ordering. Works for both unicast and multicast messages.
 * <br/>
 * Compared to {@link FRAG2}, this protocol does <em>not</em> need to serialize the message in order to break
 * it into smaller fragments: if the message is a {@link BytesMessage}, then we send all fragments with a reference to
 * the original message's byte array, plus and offset and length. Otherwise, we use a number of {@link FragmentedMessage}
 * instances, with a reference to the original message and also an offset and length. These serialize messages at the
 * last possible moment, just before being sent by the transport.
 * 
 * @author  Bela Ban
 * @version 5.0
 */
public class FRAG4 extends FRAG2 {


    protected void fragment(Message msg) {
        try {
            if(msg.getSrc() == null && local_addr != null)
                msg.setSrc(local_addr);
            int offset=msg.hasArray()? msg.getOffset() : 0, length=msg.hasArray()? msg.getLength() : msg.size();
            final List<Range> fragments=Util.computeFragOffsets(offset, length, frag_size);
            int num_frags=fragments.size();
            final long frag_id=getNextId(); // used as a seqno
            num_frags_sent.add(num_frags);
            if(log.isTraceEnabled()) {
                Address dest=msg.getDest();
                log.trace("%s: fragmenting message to %s (size=%d) into %d fragment(s) [frag_size=%d]",
                          local_addr, dest != null ? dest : "<all>", msg.getLength(), num_frags, frag_size);
            }
            for(int i=0; i < num_frags; i++) {
                Range r=fragments.get(i);
                Message frag_msg=msg.hasArray()?
                  msg.copy(false, i == 0).setArray(msg.getArray(), (int)r.low, (int)r.high)
                    .putHeader(this.id, new FragHeader(frag_id, i, num_frags))
                  : new FragmentedMessage(msg, (int)r.low, (int)r.high).setDest(msg.getDest()).setSrc(msg.getSrc())
                  .putHeader(this.id, new FragHeader(frag_id, i, num_frags).setOriginalType(msg.getType()));
                down_prot.down(frag_msg);
            }
        }
        catch(Exception e) {
            log.error("%s: fragmentation failure: %s", local_addr, e);
        }
    }


    @Override
    protected Message assembleMessage(Message[] fragments, boolean needs_deserialization, FragHeader hdr) throws Exception {
        if(fragments[0] instanceof FragmentedMessage) {
            if(Objects.equals(local_addr, fragments[0].getSrc()))
                return ((FragmentedMessage)fragments[0]).getOriginalMessage();
            InputStream seq=new SequenceInputStream(Util.enumerate(fragments, 0, fragments.length,
                                                                   m -> new ByteArrayDataInputStream(m.getArray(),
                                                                                                     m.getOffset(),
                                                                                                     m.getLength())));
            DataInput in=new DataInputStream(seq);
            Message retval=msg_factory.create(hdr.getOriginalType());
            retval.readFrom(in);
            return retval;
        }

        int combined_length=0, index=0;
        for(Message fragment: fragments)
            combined_length+=fragment.getLength();

        byte[] combined_buffer=new byte[combined_length];
        Message retval=fragments[0].copy(false, true); // doesn't copy the payload, but copies the headers

        for(int i=0; i < fragments.length; i++) {
            Message fragment=fragments[i];
            fragments[i]=null; // help garbage collection a bit
            byte[] tmp=fragment.getArray();
            int length=fragment.getLength(), offset=fragment.getOffset();
            System.arraycopy(tmp, offset, combined_buffer, index, length);
            index+=length;
        }
        return retval.setArray(combined_buffer, 0, combined_buffer.length);
    }
}


