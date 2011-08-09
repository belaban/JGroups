package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Range;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
 * message, so we add a constant (200 bytes).
 * 
 * @author Bela Ban
 */
@MBean(description="Fragments messages larger than fragmentation size into smaller packets")
public class FRAG2 extends Protocol {
    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="The max number of bytes in a message. Larger messages will be fragmented")
    int frag_size=60000;
  
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
    /*the fragmentation list contains a fragmentation table per sender
     *this way it becomes easier to clean up if a sender (member) leaves or crashes
     */
    private final ConcurrentMap<Address,ConcurrentMap<Long,FragEntry>> fragment_list=Util.createConcurrentMap(11);

    /** Used to assign fragmentation-specific sequence IDs (monotonically increasing) */
    private int curr_id=1;

    private final List<Address> members=new ArrayList<Address>(11);

    @ManagedAttribute(description="Number of sent messages")
    AtomicLong num_sent_msgs=new AtomicLong(0);
    @ManagedAttribute(description="Number of received messages")
    AtomicLong num_received_msgs=new AtomicLong(0);
    @ManagedAttribute(description="Number of sent fragments")
    AtomicLong num_sent_frags=new AtomicLong(0);
    @ManagedAttribute(description="Number of received fragments")
    AtomicLong num_received_frags=new AtomicLong(0);

    public int getFragSize() {return frag_size;}
    public void setFragSize(int s) {frag_size=s;}
    public long getNumberOfSentMessages() {return num_sent_msgs.get();}
    public long getNumberOfSentFragments() {return num_sent_frags.get();}
    public long getNumberOfReceivedMessages() {return num_received_msgs.get();}
    public long getNumberOfReceivedFragments() {return num_received_frags.get();}


    synchronized int getNextId() {
        return curr_id++;
    }  

    public void init() throws Exception {
        super.init();
        
        int old_frag_size=frag_size;
        if(frag_size <=0)
            throw new Exception("frag_size=" + old_frag_size + ", new frag_size=" + frag_size + ": new frag_size is invalid");

        TP transport=getTransport();
        if(transport != null && transport.isEnableBundling()) {
            int max_bundle_size=transport.getMaxBundleSize();
            if(frag_size >= max_bundle_size)
                throw new IllegalArgumentException("frag_size (" + frag_size + ") has to be < TP.max_bundle_size (" +
                        max_bundle_size + ")");
        }

        Map<String,Object> info=new HashMap<String,Object>(1);
        info.put("frag_size", frag_size);
        up_prot.up(new Event(Event.CONFIG, info));
        down_prot.down(new Event(Event.CONFIG, info));
    }


    public void resetStats() {
        super.resetStats();
        num_sent_msgs.set(0);
        num_sent_frags.set(0);
        num_received_frags.set(0);
        num_received_msgs.set(0);
    }



    /**
     * Fragment a packet if larger than frag_size (add a header). Otherwise just pass down. Only
     * add a header if fragmentation is needed !
     */
    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                long size=msg.getLength();
                num_sent_msgs.incrementAndGet();
                if(size > frag_size) {
                    if(log.isTraceEnabled()) {
                        log.trace(new StringBuilder("message's buffer size is ").append(size)
                                .append(", will fragment ").append("(frag_size=").append(frag_size).append(')'));
                    }
                    fragment(msg);  // Fragment and pass down
                    return null;
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.CONFIG:
                Object ret=down_prot.down(evt);
                if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
                handleConfigEvent((Map<String,Object>)evt.getArg());
                return ret;
        }

        return down_prot.down(evt);  // Pass on to the layer below us
    }


    /**
     * If event is a message, if it is fragmented, re-assemble fragments into big message and pass up
     * the stack.
     */
    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FragHeader hdr=(FragHeader)msg.getHeader(this.id);
                if(hdr != null) { // needs to be defragmented
                    unfragment(msg, hdr); // Unfragment and possibly pass up
                    return null;
                }
                else {
                    num_received_msgs.incrementAndGet();
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.CONFIG:
                Object ret=up_prot.up(evt);
                if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
                handleConfigEvent((Map<String,Object>)evt.getArg());
                return ret;
        }

        return up_prot.up(evt); // Pass up to the layer above us by default
    }


    private void handleViewChange(View view) {
        List<Address> new_mbrs=view.getMembers();
        List<Address> left_mbrs=Util.determineLeftMembers(members, new_mbrs);
        members.clear();
        members.addAll(new_mbrs);

        for(Address mbr: left_mbrs) {
            // the new view doesn't contain the sender, it must have left, hence we will clear its fragmentation tables
            fragment_list.remove(mbr);
            if(log.isTraceEnabled())
                log.trace("[VIEW_CHANGE] removed " + mbr + " from fragmentation table");
        }
    }

    @ManagedOperation(description="removes all fragments sent by mbr")
    public void clearFragmentsFor(Address mbr) {
        if(mbr == null) return;
        fragment_list.remove(mbr);
        if(log.isTraceEnabled())
            log.trace("removed " + mbr + " from fragmentation table");
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
    private void fragment(Message msg) {
        try {
            byte[] buffer=msg.getRawBuffer();
            List<Range> fragments=Util.computeFragOffsets(msg.getOffset(), msg.getLength(), frag_size);
            int num_frags=fragments.size();
            num_sent_frags.addAndGet(num_frags);

            if(log.isTraceEnabled()) {
                Address dest=msg.getDest();
                StringBuilder sb=new StringBuilder("fragmenting packet to ");
                sb.append((dest != null ? dest.toString() : "<all members>")).append(" (size=").append(buffer.length);
                sb.append(") into ").append(num_frags).append(" fragment(s) [frag_size=").append(frag_size).append(']');
                log.trace(sb.toString());
            }

            long frag_id=getNextId(); // used as a seqno
            for(int i=0; i < fragments.size(); i++) {
                Range r=fragments.get(i);
                // don't copy the buffer, only src, dest and headers. Only copy the headers one time !
                Message frag_msg=msg.copy(false, i == 0);
                frag_msg.setBuffer(buffer, (int)r.low, (int)r.high);
                FragHeader hdr=new FragHeader(frag_id, i, num_frags);
                frag_msg.putHeader(this.id, hdr);
                down_prot.down(new Event(Event.MSG, frag_msg));
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("fragmentation failure", e);
        }
    }


    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Pass msg up the stack
     */
    private void unfragment(Message msg, FragHeader hdr) {
        Address            sender=msg.getSrc();
        Message            assembled_msg=null;

        ConcurrentMap<Long,FragEntry> frag_table=fragment_list.get(sender);
        if(frag_table == null) {
            frag_table=Util.createConcurrentMap(16, .075f, 16);
            ConcurrentMap<Long,FragEntry> tmp=fragment_list.putIfAbsent(sender, frag_table);
            if(tmp != null) // value was already present
                frag_table=tmp;
        }
        num_received_frags.incrementAndGet();

        FragEntry entry=frag_table.get(hdr.id);
        if(entry == null) {
            entry=new FragEntry(hdr.num_frags);
            FragEntry tmp=frag_table.putIfAbsent(hdr.id, entry);
            if(tmp != null)
                entry=tmp;
        }

        entry.lock();
        try {
            entry.set(hdr.frag_id, msg);
            if(entry.isComplete()) {
                assembled_msg=entry.assembleMessage();
                frag_table.remove(hdr.id);
            }
        }
        finally {
            entry.unlock();
        }

        // assembled_msg=frag_table.add(hdr.id, hdr.frag_id, hdr.num_frags, msg);
        if(assembled_msg != null) {
            try {
                if(log.isTraceEnabled()) log.trace("assembled_msg is " + assembled_msg);
                assembled_msg.setSrc(sender); // needed ? YES, because fragments have a null src !!
                num_received_msgs.incrementAndGet();
                up_prot.up(new Event(Event.MSG, assembled_msg));
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("unfragmentation failed", e);
            }
        }
    }


    void handleConfigEvent(Map<String,Object> map) {
        if(map == null) return;
        if(map.containsKey("frag_size")) {
            frag_size=((Integer)map.get("frag_size")).intValue();
            if(log.isDebugEnabled()) log.debug("setting frag_size=" + frag_size);
        }
    }





    /**
     * Class represents an entry for a message. Each entry holds an array of byte arrays sorted
     * once all the byte buffer entries have been filled the fragmentation is considered complete.<br/>
     * All methods are unsynchronized, use getLock() to obtain a lock for concurrent access.
     */
    private static class FragEntry {
        // each fragment is a byte buffer
        final Message fragments[];
        //the number of fragments we have received
        int number_of_frags_recvd=0;

        private final Lock lock=new ReentrantLock();


        /**
         * Creates a new entry
         * @param tot_frags the number of fragments to expect for this message
         */
        private FragEntry(int tot_frags) {
            fragments=new Message[tot_frags];
            for(int i=0; i < tot_frags; i++)
                fragments[i]=null;
        }

        /** Use to synchronize on FragEntry */
        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
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
            /*first make a simple check*/
            if(number_of_frags_recvd < fragments.length) {
                return false;
            }
            /*then double check just in case*/
            for(Message msg: fragments) {
                if(msg == null)
                    return false;
            }
            /*all fragmentations have been received*/
            return true;
        }

        /**
         * Assembles all the fragments into one buffer. Takes all Messages, and combines their buffers into one
         * buffer.
         * This method does not check if the fragmentation is complete (use {@link #isComplete()} to verify
         * before calling this method)
         * @return the complete message in one buffer
         *
         */
        private Message assembleMessage() {
            Message retval;
            byte[]  combined_buffer, tmp;
            int     combined_length=0, length, offset;
            int     index=0;

            for(Message fragment: fragments)
                combined_length+=fragment.getLength();

            combined_buffer=new byte[combined_length];
            retval=fragments[0].copy(false); // doesn't copy the payload, but copies the headers

            for(int i=0; i < fragments.length; i++) {
                Message fragment=fragments[i];
                fragments[i]=null; // help garbage collection a bit
                tmp=fragment.getRawBuffer();
                length=fragment.getLength();
                offset=fragment.getOffset();
                System.arraycopy(tmp, offset, combined_buffer, index, length);
                index+=length;
            }

            retval.setBuffer(combined_buffer);
            return retval;
        }

        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append("[tot_frags=").append(fragments.length).append(", number_of_frags_recvd=").append(number_of_frags_recvd).append(']');
            return ret.toString();
        }

    }

}


