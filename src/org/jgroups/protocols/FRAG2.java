// $Id: FRAG2.java,v 1.12 2004/10/07 14:20:16 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Range;
import org.jgroups.util.Util;
import org.jgroups.util.Streamable;

import java.io.*;
import java.util.*;


/**
 * Fragmentation layer. Fragments messages larger than frag_size into smaller packets.
 * Reassembles fragmented packets into bigger ones. The fragmentation number is prepended
 * to the messages as a header (and removed at the receiving side).<p>
 * Each fragment is identified by (a) the sender (part of the message to which the header is appended),
 * (b) the fragmentation ID (which is unique per FRAG2 layer (monotonically increasing) and (c) the
 * fragement ID which ranges from 0 to number_of_fragments-1.<p>
 * Requirement: lossless delivery (e.g. NAK, ACK). No requirement on ordering. Works for both unicast and
 * multicast messages.<br/>
 * Compared to FRAG, this protocol does <em>not</em> need to serialize the message in order to break it into
 * smaller fragments: it looks only at the message's buffer, which is a byte[] array anyway. We assume that the
 * size addition for headers and src and dest address is minimal when the transport finally has to serialize the
 * message, so we add a constant (1000 bytes).
 * @author Bela Ban
 * @version $Id: FRAG2.java,v 1.12 2004/10/07 14:20:16 belaban Exp $
 */
public class FRAG2 extends Protocol {

    /** The max number of bytes in a message. If a message's buffer is bigger, it will be fragmented */
    int frag_size=8192;

    /** Number of bytes that we think the headers plus src and dest will take up when
        message is serialized by transport. This will be subtracted from frag_size */
    int overhead=0; // todo: set to a higher value

    /*the fragmentation list contains a fragmentation table per sender
     *this way it becomes easier to clean up if a sender (member) leaves or crashes
     */
    private final FragmentationList     fragment_list=new FragmentationList();
    private int                   curr_id=1;
    private Address               local_addr=null;
    private final Vector                members=new Vector(11);
    private static final String          name="FRAG2";


    public final String getName() {
        return name;
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("frag_size");
        if(str != null) {
            frag_size=Integer.parseInt(str);
            props.remove("frag_size");
        }

        str=props.getProperty("overhead");
        if(str != null) {
            overhead=Integer.parseInt(str);
            props.remove("overhead");
        }

        int old_frag_size=frag_size;
        frag_size-=overhead;
        if(frag_size <=0) {
            if(log.isErrorEnabled()) log.error("frag_size=" + old_frag_size + ", overhead=" + overhead +
                    ", new frag_size=" + frag_size + ": new frag_size is invalid");
            return false;
        }

            if(log.isInfoEnabled()) log.info("frag_size=" + old_frag_size + ", overhead=" + overhead +
                    ", new frag_size=" + frag_size);

        if(props.size() > 0) {
            System.err.println("FRAG2.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }




    /**
     * Fragment a packet if larger than frag_size (add a header). Otherwise just pass down. Only
     * add a header if framentation is needed !
     */
    public void down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                long size=msg.getLength();
                if(size > frag_size) {
                     {
                        StringBuffer sb=new StringBuffer("message's buffer size is ");
                        sb.append(size).append(", will fragment ").append("(frag_size=");
                        sb.append(frag_size).append(')');
                        if(log.isInfoEnabled()) log.info(sb.toString());
                    }
                    fragment(msg);  // Fragment and pass down
                    return;
                }
                break;

            case Event.VIEW_CHANGE:
                //don't do anything if this dude is sending out the view change
                //we are receiving a view change,
                //in here we check for the
                View view=(View)evt.getArg();
                Vector new_mbrs=view.getMembers(), left_mbrs;
                Address mbr;

                left_mbrs=Util.determineLeftMembers(members, new_mbrs);
                members.clear();
                members.addAll(new_mbrs);

                for(int i=0; i < left_mbrs.size(); i++) {
                    mbr=(Address)left_mbrs.elementAt(i);
                    //the new view doesn't contain the sender, he must have left,
                    //hence we will clear all his fragmentation tables
                    fragment_list.remove(mbr);

                        if(log.isInfoEnabled()) log.info("[VIEW_CHANGE] removed " + mbr + " from fragmentation table");
                }
                break;

            case Event.CONFIG:
                passDown(evt);
                 if(log.isInfoEnabled()) log.info("received CONFIG event: " + evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                return;
        }

        passDown(evt);  // Pass on to the layer below us
    }


    /**
     * If event is a message, if it is fragmented, re-assemble fragments into big message and pass up
     * the stack.
     * todo: Filip catch the view change event so that we can clean up old members
     */
    public void up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Object obj=msg.getHeader(getName());
                if(obj != null && obj instanceof FragHeader) { // needs to be defragmented
                    unfragment(msg); // Unfragment and possibly pass up
                    return;
                }
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.CONFIG:
                passUp(evt);
                 if(log.isInfoEnabled()) log.info("received CONFIG event: " + evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                return;
        }

        passUp(evt); // Pass up to the layer above us by default
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
    void fragment(Message msg) {
        byte[]             buffer;
        List               fragments;
        Event              evt;
        FragHeader         hdr;
        Message            frag_msg=null;
        Address            dest=msg.getDest();
        long               id=curr_id++; // used as seqnos
        int                num_frags=0;
        StringBuffer       sb;
        Range              r;

        try {
            buffer=msg.getBuffer();
            fragments=Util.computeFragOffsets(buffer, frag_size);
            num_frags=fragments.size();

             {
                sb=new StringBuffer("fragmenting packet to ");
                sb.append((dest != null ? dest.toString() : "<all members>")).append(" (size=").append(buffer.length);
                sb.append(") into ").append(num_frags).append(" fragment(s) [frag_size=").append(frag_size).append(']');
                if(log.isInfoEnabled()) log.info(sb.toString());
            }

            for(int i=0; i < fragments.size(); i++) {
                r=(Range)fragments.get(i);
                // Copy the original msg (needed because we need to copy the headers too)
                frag_msg=msg.copy(false); // don't copy the buffer
                frag_msg.setBuffer(buffer, (int)r.low, (int)r.high);
                hdr=new FragHeader(id, i, num_frags);

                    if(log.isDebugEnabled()) log.debug("fragment's header is " + hdr);
                frag_msg.putHeader(name, hdr);
                evt=new Event(Event.MSG, frag_msg);
                passDown(evt);
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception is " + e);
        }

    }


    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Pass msg up the stack
     */
    void unfragment(Message msg) {
        FragmentationTable frag_table=null;
        Address            sender=msg.getSrc();
        Message            assembled_msg;
        FragHeader         hdr=(FragHeader)msg.removeHeader(getName());

         if(log.isDebugEnabled()) log.debug("[" + local_addr + "] received msg, hdr is " + hdr);

        frag_table=fragment_list.get(sender);
        if(frag_table == null) {
            frag_table=new FragmentationTable(sender);
            try {
                fragment_list.add(sender, frag_table);
            }
            catch(IllegalArgumentException x) { // the entry has already been added, probably in parallel from another thread
                frag_table=fragment_list.get(sender);
            }
        }
        assembled_msg=frag_table.add(hdr.id, hdr.frag_id, hdr.num_frags, msg);
        if(assembled_msg != null) {
            try {
                 if(log.isInfoEnabled()) log.info("assembled_msg is " + assembled_msg);
                assembled_msg.setSrc(sender); // needed ? YES, because fragments have a null src !!
                passUp(new Event(Event.MSG, assembled_msg));
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("exception is " + e);
            }
        }
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("frag_size")) {
            frag_size=((Integer)map.get("frag_size")).intValue();

                if(log.isInfoEnabled()) log.info("setting frag_size=" + frag_size);
        }
    }


    public static class FragHeader extends Header implements Streamable {
        public long id=0;
        public int frag_id=0;
        public int num_frags=0;

        public FragHeader() {
        } // used for externalization

        public FragHeader(long id, int frag_id, int num_frags) {
            this.id=id;
            this.frag_id=frag_id;
            this.num_frags=num_frags;
        }

        public String toString() {
            return "[FRAG2: id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + ']';
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(id);
            out.writeInt(frag_id);
            out.writeInt(num_frags);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id=in.readLong();
            frag_id=in.readInt();
            num_frags=in.readInt();
        }

       public void writeTo(DataOutputStream out) throws IOException {
            out.writeLong(id);
            out.writeInt(frag_id);
            out.writeInt(num_frags);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            id=in.readLong();
            frag_id=in.readInt();
            num_frags=in.readInt();
        }

    }


    /**
     * A fragmentation list keeps a list of fragmentation tables
     * sorted by an Address ( the sender ).
     * This way, if the sender disappears or leaves the group half way
     * sending the content, we can simply remove this members fragmentation
     * table and clean up the memory of the receiver.
     * We do not have to do the same for the sender, since the sender doesn't keep a fragmentation table
     */
    class FragmentationList {
        /* initialize the hashtable to hold all the fragmentation
         * tables
         * 11 is the best growth capacity to start with
         */
        private final Hashtable frag_tables=new Hashtable(11);


        /**
         * Adds a fragmentation table for this particular sender
         * If this sender already has a fragmentation table, an IllegalArgumentException
         * will be thrown.
         * @param   sender - the address of the sender, cannot be null
         * @param   table - the fragmentation table of this sender, cannot be null
         * @exception IllegalArgumentException if an entry for this sender already exist
         */
        public synchronized void add(Address sender, FragmentationTable table) throws IllegalArgumentException {
            FragmentationTable healthCheck=(FragmentationTable)frag_tables.get(sender);
            if(healthCheck == null) {
                frag_tables.put(sender, table);
            }
            else {
                throw new IllegalArgumentException("Sender <" + sender + "> already exists in the fragementation list.");
            }
        }

        /**
         * returns a fragmentation table for this sender
         * returns null if the sender doesn't have a fragmentation table
         * @return the fragmentation table for this sender, or null if no table exist
         */
        public FragmentationTable get(Address sender) {
            return (FragmentationTable)frag_tables.get(sender);
        }


        /**
         * returns true if this sender already holds a
         * fragmentation for this sender, false otherwise
         * @param sender - the sender, cannot be null
         * @return true if this sender already has a fragmentation table
         */
        public boolean containsSender(Address sender) {
            return frag_tables.containsKey(sender);
        }

        /**
         * removes the fragmentation table from the list.
         * after this operation, the fragementation list will no longer
         * hold a reference to this sender's fragmentation table
         * @param sender - the sender who's fragmentation table you wish to remove, cannot be null
         * @return true if the table was removed, false if the sender doesn't have an entry
         */
        public synchronized boolean remove(Address sender) {
            boolean result=containsSender(sender);
            frag_tables.remove(sender);
            return result;
        }

        /**
         * returns a list of all the senders that have fragmentation tables
         * opened.
         * @return an array of all the senders in the fragmentation list
         */
        public synchronized Address[] getSenders() {
            Address[] result=new Address[frag_tables.size()];
            java.util.Enumeration en=frag_tables.keys();
            int index=0;
            while(en.hasMoreElements()) {
                result[index++]=(Address)en.nextElement();
            }
            return result;
        }

        public String toString() {
            java.util.Enumeration e=frag_tables.elements();
            StringBuffer buf=new StringBuffer("Fragmentation list contains ")
                    .append(frag_tables.size()).append(" tables\n");
            while(e.hasMoreElements()) {
                buf.append(e.nextElement());
            }
            return buf.toString();
        }

    }

    /**
     * Keeps track of the fragments that are received.
     * Reassembles fragements into entire messages when all fragments have been received.
     * The fragmentation holds a an array of byte arrays for a unique sender
     * The first dimension of the array is the order of the fragmentation, in case the arrive out of order
     */
    class FragmentationTable {
        private final Address sender;
        /* the hashtable that holds the fragmentation entries for this sender*/
        private final Hashtable h=new Hashtable(11);  // keys: frag_ids, vals: Entrys


        public FragmentationTable(Address sender) {
            this.sender=sender;
        }


        /**
         * inner class represents an entry for a message
         * each entry holds an array of byte arrays sorted
         * once all the byte buffer entries have been filled
         * the fragmentation is considered complete.
         */
        class Entry {
            //the total number of fragment in this message
            int tot_frags=0;
            // each fragment is a byte buffer
            Message fragments[]=null;
            //the number of fragments we have received
            int number_of_frags_recvd=0;
            // the message ID
            long msg_id=-1;

            /**
             * Creates a new entry
             * @param tot_frags the number of fragments to expect for this message
             */
            Entry(long msg_id, int tot_frags) {
                this.msg_id=msg_id;
                this.tot_frags=tot_frags;
                fragments=new Message[tot_frags];
                for(int i=0; i < tot_frags; i++)
                    fragments[i]=null;
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
                /*first make the simple check*/
                if(number_of_frags_recvd < tot_frags) {
                    return false;
                }
                /*then double check just in case*/
                for(int i=0; i < fragments.length; i++) {
                    if(fragments[i] == null)
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
            public Message assembleMessage() {
                Message retval=null;
                byte[]  combined_buffer, tmp;
                int     combined_length=0, length, offset;
                Message fragment;
                int     index=0;

                for(int i=0; i < fragments.length; i++) {
                    fragment=fragments[i];
                    combined_length+=fragment.getLength();
                }

                combined_buffer=new byte[combined_length];
                for(int i=0; i < fragments.length; i++) {
                    fragment=fragments[i];
                    tmp=fragment.getRawBuffer();
                    length=fragment.getLength();
                    offset=fragment.getOffset();
                    System.arraycopy(tmp, offset, combined_buffer, index, length);
                    index+=length;
                }

                retval=fragments[0].copy(false);
                retval.setBuffer(combined_buffer);
                return retval;
            }

            /**
             * debug only
             */
            public String toString() {
                StringBuffer ret=new StringBuffer();
                ret.append("[tot_frags=" + tot_frags + ", number_of_frags_recvd=" + number_of_frags_recvd + ']');
                return ret.toString();
            }

            public int hashCode() {
                return super.hashCode();
            }
        }


        /**
         * Creates a new entry if not yet present. Adds the fragment.
         * If all fragements for a given message have been received,
         * an entire message is reassembled and returned.
         * Otherwise null is returned.
         * @param   id - the message ID, unique for a sender
         * @param   frag_id the index of this fragmentation (0..tot_frags-1)
         * @param   tot_frags the total number of fragmentations expected
         * @param   fragment - the byte buffer for this fragment
         */
        public synchronized Message add(long id, int frag_id, int tot_frags, Message fragment) {
            Message retval=null;

            Entry e=(Entry)h.get(new Long(id));

            if(e == null) {   // Create new entry if not yet present
                e=new Entry(id, tot_frags);
                h.put(new Long(id), e);
            }

            e.set(frag_id, fragment);
            if(e.isComplete()) {
                retval=e.assembleMessage();
                h.remove(new Long(id));
            }

            return retval;
        }


        public void reset() {
        }

        public String toString() {
            StringBuffer buf=new StringBuffer("Fragmentation Table Sender:").append(sender).append("\n\t");
            java.util.Enumeration e=this.h.elements();
            while(e.hasMoreElements()) {
                Entry entry=(Entry)e.nextElement();
                int count=0;
                for(int i=0; i < entry.fragments.length; i++) {
                    if(entry.fragments[i] != null) {
                        count++;
                    }
                }
                buf.append("Message ID:").append(entry.msg_id).append("\n\t");
                buf.append("Total Frags:").append(entry.tot_frags).append("\n\t");
                buf.append("Frags Received:").append(count).append("\n\n");
            }
            return buf.toString();
        }
    }

}


