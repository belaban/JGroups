package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.FixedSizeBitSet;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Fragmentation protocol which uses less memory to store fragments than {@link FRAG2}.<br/>
 * When a message is fragmented, all fragments carry the size of the original message, their offset and
 * length with respect to the original message and a fragment ID (to identify the fragment).<br/>
 * When the first fragment is received, the full message is created and each fragment copies its data into the full
 * message at its offset and length. When all fragments have been received, the full message is passed up.<br/>
 * Only the first fragment carries the headers and dest and src addresses. When received, its src/dest addresses and
 * the headers will be set in the full message.<br/>
 * For details see https://issues.jboss.org/browse/JGRP-2154
 * <br/>
 * Requirement: lossless delivery (e.g. NAKACK2 or UNICAST3). No requirement on ordering. Works for both unicast and
 * multicast messages.<br/>
 *
 * @author Bela Ban
 * @since 4.0
 */
@MBean(description="Fragments messages larger than fragmentation size into smaller packets")
public class FRAG3 extends Protocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="The max number of bytes in a message. Larger messages will be fragmented")
    protected int                 frag_size=60000;
  
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
    /*the fragmentation list contains a fragmentation table per sender
     *this way it becomes easier to clean up if a sender (member) leaves or crashes
     */
    protected final ConcurrentMap<Address,ConcurrentMap<Integer,FragEntry>> fragment_list=Util.createConcurrentMap(11);

    // Used to assign fragmentation-specific sequence IDs (monotonically increasing)
    protected final AtomicInteger curr_id=new AtomicInteger(1);

    protected final List<Address> members=new ArrayList<>(11);

    protected Address             local_addr;

    @ManagedAttribute(description="Number of sent fragments")
    protected LongAdder           num_frags_sent=new LongAdder();
    @ManagedAttribute(description="Number of received fragments")
    protected LongAdder           num_frags_received=new LongAdder();

    protected final AverageMinMax avg_size_down=new AverageMinMax();
    protected final AverageMinMax avg_size_up=new AverageMinMax();

    public int   getFragSize()                  {return frag_size;}
    public void  setFragSize(int s)             {frag_size=s;}
    public long  getNumberOfSentFragments()     {return num_frags_sent.sum();}
    public long  getNumberOfReceivedFragments() {return num_frags_received.sum();}
    public int   fragSize()                     {return frag_size;}
    public FRAG3 fragSize(int size)             {frag_size=size; return this;}

    @ManagedAttribute(description="min/avg/max size (in bytes) for messages sent down that needed to be fragmented")
    public String getAvgSizeDown() {return avg_size_down.toString();}

    @ManagedAttribute(description="min/avg/max size (in bytes) of messages re-assembled from fragments")
    public String getAvgSizeUp()   {return avg_size_up.toString();}

    protected int getNextId() {return curr_id.getAndIncrement();}

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

        Map<String,Object> info=new HashMap<>(1);
        info.put("frag_size", frag_size);
        down_prot.down(new Event(Event.CONFIG, info));
    }


    public void resetStats() {
        super.resetStats();
        num_frags_sent.reset();
        num_frags_received.reset();
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
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);  // Pass on to the layer below us
    }

    public Object down(Message msg) {
        int size=msg.getLength();
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
        Frag3Header hdr=msg.getHeader(this.id);
        if(hdr != null) { // needs to be defragmented
            Message assembled_msg=unfragment(msg, hdr);
            if(assembled_msg != null) {
                assembled_msg.setSrc(msg.getSrc()); // needed ? YES, because fragments have a null src !!
                up_prot.up(assembled_msg);
                avg_size_up.add(assembled_msg.length());
            }
            return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            Frag3Header hdr=msg.getHeader(this.id);
            if(hdr != null) { // needs to be defragmented
                Message assembled_msg=unfragment(msg,hdr);
                if(assembled_msg != null) {
                    // the reassembled msg has to be add in the right place (https://issues.jboss.org/browse/JGRP-1648),
                    // and canot be added to the tail of the batch !
                    batch.replace(msg, assembled_msg);
                    avg_size_up.add(assembled_msg.length());
                }
                else
                    batch.remove(msg);
            }
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
            byte[] buffer=msg.getRawBuffer();
            int original_length=msg.getLength();
            int num_frags=(int)Math.ceil(original_length /(double)frag_size);
            num_frags_sent.add(num_frags);

            if(log.isTraceEnabled()) {
                Address dest=msg.getDest();
                log.trace("%s: fragmenting message to %s (size=%d) into %d fragment(s) [frag_size=%d]",
                          local_addr, dest != null ? dest : "<all>", original_length, num_frags, frag_size);
            }

            int frag_id=getNextId(); // used as the common ID for all fragments of this message
            int total_size=original_length + msg.getOffset();
            int offset=msg.getOffset();
            int tmp_size=0, i=0;

            while(offset < total_size) {
                if(offset + frag_size <= total_size)
                    tmp_size=frag_size;
                else
                    tmp_size=total_size - offset;

                Frag3Header hdr=new Frag3Header(frag_id, i, num_frags, original_length,
                                                offset - msg.getOffset()); // at the receiver, offset needs to start at 0!!

                // don't copy the buffer, only src, dest and headers. Only copy the headers for the first fragment!
                Message frag_msg=msg.copy(false, i == 0).setBuffer(buffer, offset, tmp_size).putHeader(this.id, hdr);
                down_prot.down(frag_msg);
                offset+=tmp_size;
                i++;
            }
        }
        catch(Exception e) {
            log.error(String.format("%s: fragmentation failure", local_addr), e);
        }
    }



    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Return the message
     */
    protected Message unfragment(Message msg, Frag3Header hdr) {
        Address   sender=msg.getSrc();
        Message   assembled_msg=null;

        ConcurrentMap<Integer,FragEntry> frag_table=fragment_list.get(sender);
        if(frag_table == null) {
            frag_table=Util.createConcurrentMap(16, .075f, 16);
            ConcurrentMap<Integer,FragEntry> tmp=fragment_list.putIfAbsent(sender, frag_table);
            if(tmp != null) // value was already present
                frag_table=tmp;
        }
        num_frags_received.increment();

        FragEntry entry=frag_table.get(hdr.id);
        if(entry == null) {
            entry=new FragEntry(hdr.num_frags);
            FragEntry tmp=frag_table.putIfAbsent(hdr.id, entry);
            if(tmp != null)
                entry=tmp;
        }

        if((assembled_msg=entry.set(msg, hdr)) != null) {
            frag_table.remove(hdr.id);
            if(log.isTraceEnabled())
                log.trace("%s: unfragmented message from %s (size=%d) from %d fragments",
                          local_addr, sender, assembled_msg.getLength(), entry.num_frags);
        }
        return assembled_msg;
    }





    /**
     * Entry for a full message, received fragments are copied into buffer and set in the bitset of expected frags.
     * When complete, the buffer is set in the resulting message and the message returned.
     */
    protected static class FragEntry {
        protected final Lock            lock=new ReentrantLock();

        // the message to be passed up; fragments write their payloads into the buffer at the correct offsets
        protected       Message         msg;
        protected       byte[]          buffer;
        protected final int             num_frags; // number of expected fragments
        protected final FixedSizeBitSet received;


        /**
         * Creates a new entry
         * @param num_frags the number of fragments expected for this message
         */
        protected FragEntry(int num_frags) {
            this.num_frags=num_frags;
            received=new FixedSizeBitSet(num_frags);
        }


        /** Adds a fragment to the full message */
        public Message set(Message frag_msg, Frag3Header hdr) {
            lock.lock();
            try {
                if(buffer == null)
                    buffer=new byte[hdr.original_length];

                if(hdr.frag_id == 0) {
                    // the first fragment creates the message, copy the headers but not the buffer
                    msg=frag_msg.copy(false);
                }

                if(received.set(hdr.frag_id)) {
                    // if not yet added: copy the fragment's buffer into msg.buffer at the correct offset
                    int frag_length=frag_msg.getLength();
                    int offset=hdr.offset;
                    System.arraycopy(frag_msg.getRawBuffer(), frag_msg.getOffset(), buffer, offset, frag_length);
                    if(isComplete())
                        return assembleMessage();
                }
                return null;
            }
            finally {
                lock.unlock();
            }
        }

        /** Returns true if this fragmentation is complete, ie all fragments have been received for this buffer */
        protected boolean isComplete() {
            return received.cardinality() == num_frags;
        }

        /**
         * Assembles all the fragments into one buffer. Takes all Messages, and combines their buffers into one buffer.
         * @return the complete message in one buffer
         */
        protected Message assembleMessage() {
            return msg.setBuffer(buffer);
        }

        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append("[tot_frags=").append(num_frags).append(", number_of_frags_recvd=").append(received.cardinality()).append(']');
            return ret.toString();
        }

    }

}


