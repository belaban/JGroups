package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;


/**
 * Fragmentation layer. Fragments messages larger than frag_size into smaller
 * packets. Reassembles fragmented packets into bigger ones. The fragmentation
 * number is prepended to the messages as a header (and removed at the receiving
 * side).
 * <p>
 * Each fragment is identified by (a) the sender (part of the message to which
 * the header is appended), (b) the fragmentation ID (which is unique per FRAG2
 * layer (monotonically increasing) and (c) the fragement ID which ranges from 0
 * to number_of_fragments-1.
 * <p>
 * Requirement: lossless delivery (e.g. NAK, ACK). No requirement on ordering.
 * Works for both unicast and multicast messages.<br/> Compared to FRAG, this
 * protocol does <em>not</em> need to serialize the message in order to break
 * it into smaller fragments: it looks only at the message's buffer, which is a
 * byte[] array anyway. We assume that the size addition for headers and src and
 * dest address is minimal when the transport finally has to serialize the
 * message, so we add a constant (200 bytes).</br>
 * <em>Note that this protocol only works with {@link BytesMessage} types</em>
 * 
 * @author Bela Ban
 */
public class FRAG2 extends Fragmentation {

    // fragmentation list has a fragtable per sender; this way it becomes easier to clean up if a member leaves or crashes
    protected final ConcurrentMap<Address,ConcurrentMap<Long,FragEntry>> fragment_list=Util.createConcurrentMap(11);

    protected final Predicate<Message> HAS_FRAG_HEADER=msg -> msg.getHeader(id) != null;

    /** Used to assign fragmentation-specific sequence IDs (monotonically increasing) */
    protected int                 curr_id=1;

    protected final List<Address> members=new ArrayList<>(11);
    protected MessageFactory      msg_factory;

    protected final AverageMinMax avg_size_down=new AverageMinMax();
    protected final AverageMinMax avg_size_up=new AverageMinMax();

    @ManagedAttribute(description="min/avg/max size (in bytes) for messages sent down that needed to be fragmented")
    public String getAvgSizeDown() {return avg_size_down.toString();}

    @ManagedAttribute(description="min/avg/max size (in bytes) of messages re-assembled from fragments")
    public String getAvgSizeUp()   {return avg_size_up.toString();}

    synchronized int getNextId() {
        return curr_id++;
    }  

    public void init() throws Exception {
        super.init();
        
        int old_frag_size=frag_size;
        if(frag_size <=0)
            throw new Exception("frag_size=" + old_frag_size + ", new frag_size=" + frag_size + ": new frag_size is invalid");

        TP transport=getTransport();
        if(transport != null) {
            int max_bundle_size=transport.getMaxBundleSize();
            if(frag_size >= max_bundle_size)
                throw new IllegalArgumentException("frag_size (" + frag_size + ") has to be < TP.max_bundle_size (" +
                                                     max_bundle_size + ")");
        }
        msg_factory=transport.getMessageFactory();
        Map<String,Object> info=new HashMap<>(1);
        info.put("frag_size", frag_size);
        down_prot.down(new Event(Event.CONFIG, info));
    }


    public void resetStats() {
        super.resetStats();
        avg_size_down.clear();
        avg_size_up.clear();
    }



    /**
     * Fragment a packet if larger than frag_size (add a header). Otherwise just pass down. Only
     * add a header if fragmentation is needed !
     */
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;
        }
        return super.down(evt);
    }

    public Object down(Message msg) {
        long size=msg.getLength();
        if(size > frag_size) {
            fragment(msg);  // Fragment and pass down
            avg_size_down.add(size);
            return null;
        }
        return down_prot.down(msg);
    }

    /**
     * If event is a message, if it is fragmented, re-assemble fragments into big message and pass up the stack.
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;
        }
        return up_prot.up(evt); // Pass up to the layer above us by default
    }

    public Object up(Message msg) {
        FragHeader hdr=msg.getHeader(this.id);
        if(hdr != null) { // needs to be defragmented
            Message assembled_msg=unfragment(msg, hdr);
            if(assembled_msg != null) {
                assembled_msg.setSrc(msg.getSrc()); // needed ? YES, because fragments have a null src !!
                up_prot.up(assembled_msg);
                avg_size_up.add(assembled_msg.getLength());
            }
            return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        MessageIterator it=batch.iteratorWithFilter(HAS_FRAG_HEADER);
        while(it.hasNext()) {
            Message msg=it.next();
            FragHeader hdr=msg.getHeader(this.id);
            Message assembled_msg=unfragment(msg, hdr);
            if(assembled_msg != null) {
                // the reassembled msg has to be add in the right place (https://issues.jboss.org/browse/JGRP-1648),
                // and cannot be added to the tail of the batch!
                assembled_msg.setSrc(batch.sender());
                it.replace(assembled_msg);
                avg_size_up.add(assembled_msg.getLength());
            }
            else
                it.remove();
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleViewChange(View view) {
        List<Address> new_mbrs=view.getMembers();
        List<Address> left_mbrs=Util.determineLeftMembers(members, new_mbrs);
        members.clear();
        members.addAll(new_mbrs);

        for(Address mbr: left_mbrs) {
            // the new view doesn't contain the sender, it must have left, hence we will clear its fragmentation tables
            fragment_list.remove(mbr);
            log.trace("%s: removed %s from fragmentation table", local_addr, mbr);
        }
    }

    @ManagedOperation(description="removes all fragments sent by mbr")
    public void clearFragmentsFor(Address mbr) {
        if(mbr == null) return;
        fragment_list.remove(mbr);
        log.trace("%s: removed %s from fragmentation table", local_addr, mbr);
    }

    @ManagedOperation(description="Removes all entries from the fragmentation table. " +
            "Dangerous: this might remove fragments that are still needed to assemble an entire message")
     public void clearAllFragments() {
        fragment_list.clear();
    }

    /** Send all fragments as separate messages (with same ID !).
     Example:
     <pre>
     Given the generated ID is 2344, number of fragments=3, message {dst,src,buf}
     would be fragmented into:

     [2344,3,0]{dst,src,buf1},
     [2344,3,1]{dst,src,buf2} and
     [2344,3,2]{dst,src,buf3}
     </pre>
     */
    protected void fragment(Message msg) {
        try {
            boolean serialize=!msg.hasArray();
            ByteArray tmp=null;
            byte[] buffer=serialize? (tmp=Util.messageToBuffer(msg)).getArray() : msg.getArray();
            int offset=serialize? tmp.getOffset() : msg.getOffset();
            int length=serialize? tmp.getLength() : msg.getLength();
            final List<Range> fragments=Util.computeFragOffsets(offset, length, frag_size);
            int num_frags=fragments.size();
            num_frags_sent.add(num_frags);

            if(log.isTraceEnabled()) {
                Address dest=msg.getDest();
                log.trace("%s: fragmenting message to %s (size=%d) into %d fragment(s) [frag_size=%d]",
                          local_addr, dest != null ? dest : "<all>", msg.getLength(), num_frags, frag_size);
            }

            long frag_id=getNextId(); // used as a seqno
            for(int i=0; i < num_frags; i++) {
                Range r=fragments.get(i);
                // don't copy the buffer, only src, dest and headers. Only copy the headers one time !
                Message frag_msg=null;
                if(serialize)
                    frag_msg=new BytesMessage(msg.getDest());
                else
                    frag_msg=msg.copy(false, i == 0);

                frag_msg.setArray(buffer, (int)r.low, (int)r.high);
                FragHeader hdr=new FragHeader(frag_id, i, num_frags).needsDeserialization(serialize);
                frag_msg.putHeader(this.id, hdr);
                down_prot.down(frag_msg);
            }
        }
        catch(Exception e) {
            log.error("%s: fragmentation failure: %s", local_addr, e);
        }
    }


    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Return the message
     */
    protected Message unfragment(Message msg, FragHeader hdr) {
        Address sender=msg.getSrc();
        Message assembled_msg=null;

        ConcurrentMap<Long,FragEntry> frag_table=fragment_list.get(sender);
        if(frag_table == null) {
            frag_table=Util.createConcurrentMap(16, .075f, 16);
            ConcurrentMap<Long,FragEntry> tmp=fragment_list.putIfAbsent(sender, frag_table);
            if(tmp != null) // value was already present
                frag_table=tmp;
        }
        num_frags_received.increment();

        FragEntry entry=frag_table.get(hdr.id);
        if(entry == null) {
            entry=new FragEntry(hdr.num_frags, hdr.needs_deserialization, msg_factory);
            FragEntry tmp=frag_table.putIfAbsent(hdr.id, entry);
            if(tmp != null)
                entry=tmp;
        }

        Lock lock=entry.lock;
        lock.lock();
        try {
            entry.set(hdr.frag_id, msg);
            if(entry.isComplete()) {
                assembled_msg=assembleMessage(entry.fragments, entry.needs_deserialization, hdr);
                frag_table.remove(hdr.id);
                if(log.isTraceEnabled())
                    log.trace("%s: unfragmented message from %s (size=%d) from %d fragments",
                              local_addr, sender, assembled_msg.getLength(), entry.number_of_frags_recvd);
            }
        }
        catch(Exception ex) {
            log.error("%s: failed unfragmenting message: %s", local_addr, ex);
        }
        finally {
            lock.unlock();
        }
        return assembled_msg;
    }



    /**
     * Assembles all the message fragments into one message.
     * This method does not check if the fragmentation is complete (use {@link FragEntry#isComplete()} to verify)
     * before calling this method)
     * @return the complete message
     */
    protected Message assembleMessage(Message[] fragments, boolean needs_deserialization, FragHeader hdr) throws Exception {
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
        if(needs_deserialization)
            retval=Util.messageFromBuffer(combined_buffer, 0, combined_buffer.length, msg_factory);
        else
            retval.setArray(combined_buffer, 0, combined_buffer.length);
        return retval;
    }




    /**
     * Class represents an entry for a message. Each entry holds an array of byte arrays sorted
     * once all the byte buffer entries have been filled the fragmentation is considered complete.<br/>
     * All methods are unsynchronized, use getLock() to obtain a lock for concurrent access.
     */
    protected static class FragEntry {
        protected final Message[]      fragments;
        protected int                  number_of_frags_recvd;
        protected final boolean        needs_deserialization;
        protected final MessageFactory msg_factory;
        protected final Lock           lock=new ReentrantLock();


        /**
         * Creates a new entry
         * @param tot_frags the number of fragments to expect for this message
         */
        protected FragEntry(int tot_frags, boolean needs_deserialization, MessageFactory mf) {
            fragments=new Message[tot_frags];
            this.needs_deserialization=needs_deserialization;
            this.msg_factory=mf;
        }


        /**
         * adds on fragmentation buffer to the message
         * @param frag_id the number of the fragment being added 0..(tot_num_of_frags - 1)
         * @param frag the byte buffer containing the data for this fragmentation, should not be null
         */
        public void set(int frag_id, Message frag) {
            // don't count an already received fragment (should not happen though because the
            // reliable transmission protocol(s) below should weed out duplicates
            if(fragments[frag_id] == null) {
                fragments[frag_id]=frag;
                number_of_frags_recvd++;
            }
        }

        /** returns true if this fragmentation is complete
         *  ie, all fragmentations have been received for this buffer
         *
         */
        public boolean isComplete() {
            /* first a simple check */
            if(number_of_frags_recvd < fragments.length)
                return false;

            /* then double-check just in case */
            for(Message msg: fragments) {
                if(msg == null)
                    return false;
            }
            return true;
        }


        public String toString() {
            return String.format("[tot_frags=%d, number_of_frags_recvd=%d]", fragments.length, number_of_frags_recvd);
        }

    }

}


