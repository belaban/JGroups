// $Id: FRAG.java,v 1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;


/**
 * todo
 * Filip Hanik - To do - Done as of 2001-04-07
 * 1. Make sure that the fragmentation table set also stores the member not only the message id
 *    this way I can keep track of the member to find out if he/she crashes
 * 2. Then catch the view change event.
 * 3. If there is a view change event and the new view doesn't contain the member
 *    that has a message in the FRAG stack, clean up the FRAG stack
 *
 */


/**
 * Fragmentation layer. Fragments messages larger than FRAG_SIZE into smaller packets.
 * Reassembles fragmented packets into bigger ones. The fragmentation number is prepended
 * to the messages as a header (and removed at the receiving side).<p>
 * Each fragment is identified by (a) the sender (part of the message to which the header is appended),
 * (b) the fragmentation ID (which is unique per FRAG layer (monotonically increasing) and (c) the
 * fragement ID which ranges from 0 to number_of_fragments-1.<p>
 * Requirement: lossless delivery (e.g. NAK, ACK). No requirement on ordering. Works for both unicast and
 * multicast messages.
 *
 * Typical stack:
 * <pre>
 * FIFO
 * FRAG
 * NAK
 * UDP
 * </pre>
 * @author Bela Ban
 * @author Filip Hanik
 */
public class FRAG extends Protocol {
    private int frag_size=8192;  // conservative value

    /*the fragmentation list contains a fragmentation table per sender
     *this way it becomes easier to clean up if a sender (member) leaves or crashes
     */
    private FragmentationList     fragment_list=new FragmentationList();
    private int                   curr_id=1;
    private Address               local_addr=null;
    private ByteArrayOutputStream bos=new ByteArrayOutputStream(frag_size);  // to serialize messages to be fragmented
    private Vector                members=new Vector();


    public String getName() {
        return "FRAG";
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("frag_size");
        if(str != null) {
            frag_size=new Integer(str).intValue();
            props.remove("frag_size");
        }

        if(props.size() > 0) {
            System.err.println("FRAG.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    /** Just remove if you don't need to reset any state */
    public void reset() {
        //frag_table.reset();
    }


    /**
     * Fragment a packet if larger than frag_size (add a header). Otherwise just pass down. Only
     * add a header if framentation is needed !
     */
    public void down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                long size=msg.size();
                if(size > frag_size) {
                    if(Trace.trace)
                        Trace.info("FRAG.down()", "message size is " + size + ", will fragment " +
                                "(frag_size == " + frag_size + ")");
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
                    if(Trace.trace)
                        Trace.info("FRAG.down()", "[VIEW_CHANGE] removed " + mbr + " from fragmentation table");
                }
                break;

            case Event.CONFIG:
                passDown(evt);
                if(Trace.trace) Trace.info("FRAG.down()", "received CONFIG event: " + evt.getArg());
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
                if(Trace.trace) Trace.info("FRAG.up()", "received CONFIG event: " + evt.getArg());
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
    private void fragment(Message msg) {
        ObjectOutputStream oos;
        byte[] buffer;
        byte[] fragments[];
        Event evt;
        FragHeader hdr;
        Message frag_msg=null;
        Address dest=msg.getDest(), src=msg.getSrc();
        long id=curr_id++; // used as seqnos
        int num_frags=0;


        try {
            // Write message into a byte buffer and fragment it
            bos.reset();
            oos=new ObjectOutputStream(bos);
            msg.writeExternal(oos);
            oos.flush();
            buffer=bos.toByteArray();
            fragments=Util.fragmentBuffer(buffer, frag_size);
            num_frags=fragments.length;

            if(Trace.trace)
                Trace.info("FRAG.fragment()", "fragmenting packet to " + (dest != null ? dest.toString() : "<all members>") +
                        " (size=" + buffer.length + ") into " + num_frags + " fragment(s) [frag_size=" + frag_size + "]");

            for(int i=0; i < num_frags; i++) {
                frag_msg=new Message(dest, src, fragments[i]);
                hdr=new FragHeader(id, i, num_frags);
                if(Trace.trace)
                    Trace.debug("FRAG.fragment()", "fragment's header is " + hdr);
                frag_msg.putHeader(getName(), hdr);
                evt=new Event(Event.MSG, frag_msg);
                passDown(evt);
            }
        }
        catch(Exception e) {
            Trace.error("FRAG.fragment()", "exception is " + e);
        }

    }


    /**
     1. Get all the fragment buffers
     2. When all are received -> Assemble them into one big buffer
     3. Read headers and byte buffer from big buffer
     4. Set headers and buffer in msg
     5. Pass msg up the stack
     */
    private void unfragment(Message msg) {
        FragmentationTable frag_table=null;
        Address sender=msg.getSrc();
        Message assembled_msg;
        FragHeader hdr=(FragHeader)msg.removeHeader(getName());
        byte[] m;
        ByteArrayInputStream bis;
        ObjectInputStream ois;


        if(Trace.trace) Trace.debug("FRAG.unfragment()", "[" + local_addr + "] received msg, hdr is " + hdr);

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
        m=frag_table.add(hdr.id, hdr.frag_id, hdr.num_frags, msg.getBuffer());
        if(m != null) {
            try {
                bis=new ByteArrayInputStream(m);
                ois=new ObjectInputStream(bis);
                assembled_msg=new Message();
                assembled_msg.readExternal(ois);
                if(Trace.trace) Trace.info("FRAG.unfragment()", "assembled_msg is " + assembled_msg);
                assembled_msg.setSrc(sender); // needed ? YES, because fragments have a null src !!
                passUp(new Event(Event.MSG, assembled_msg));
            }
            catch(Exception e) {
                Trace.error("FRAG.unfragment()", "exception is " + e);
            }
        }
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("frag_size")) {
            frag_size=((Integer)map.get("frag_size")).intValue();
        }
    }


    public static class FragHeader extends Header {
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
            return "[FRAG: id=" + id + ", frag_id=" + frag_id + ", num_frags=" + num_frags + "]";
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
        private Hashtable frag_tables=new Hashtable(11);


        /**
         * Adds a fragmentation table for this particular sender
         * If this sender already has a fragmentation table, an IllegalArgumentException
         * will be thrown.
         * @param   sender - the address of the sender, cannot be null
         * @param   table - the fragmentation table of this sender, cannot be null
         * @exception IllegalArgumentException if an entry for this sender already exist
         */
        public synchronized void add(Address sender,
                                     FragmentationTable table)
                throws IllegalArgumentException {
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
            java.util.Enumeration enum=frag_tables.keys();
            int index=0;
            while(enum.hasMoreElements()) {
                result[index++]=(Address)enum.nextElement();
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
        private Address sender;
        /* the hashtable that holds the fragmentation entries for this sender*/
        private Hashtable h=new Hashtable(11);  // keys: frag_ids, vals: Entrys


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
            byte[] fragments[]=null;
            //the number of fragments we have received
            int number_of_frags_recvd=0;
            // the message ID
            long msg_id=-1;

            /**
             * Creates a new entry
             * @param tot_frags the number of fragments to expect for this message
             */
            Entry(long msg_id,
                  int tot_frags) {
                this.msg_id=msg_id;
                this.tot_frags=tot_frags;
                fragments=new byte[tot_frags][];
                for(int i=0; i < tot_frags; i++) {
                    fragments[i]=null;
                }
            }

            /**
             * adds on fragmentation buffer to the message
             * @param frag_id the number of the fragment being added 0..(tot_num_of_frags - 1)
             * @param frag the byte buffer containing the data for this fragmentation, should not be null
             */
            public void set(int frag_id, byte[] frag) {
                fragments[frag_id]=frag;
                number_of_frags_recvd++;
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
             * Assembles all the fragmentations into one buffer
             * this method does not check if the fragmentation is complete
             * @return the complete message in one buffer
             *
             */
            public byte[] assembleBuffer() {
                return Util.defragmentBuffer(fragments);
            }

            /**
             * debug only
             */
            public String toString() {
                StringBuffer ret=new StringBuffer();
                ret.append("[tot_frags=" + tot_frags + ", number_of_frags_recvd=" + number_of_frags_recvd + "]");
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
        public synchronized byte[] add(long id,
                                       int frag_id,
                                       int tot_frags,
                                       byte[] fragment) {

            /*initialize the return value to default
             *not complete
             */
            byte[] retval=null;

            Entry e=(Entry)h.get(new Long(id));

            if(e == null) {   // Create new entry if not yet present
                e=new Entry(id, tot_frags);
                h.put(new Long(id), e);
            }

            e.set(frag_id, fragment);
            if(e.isComplete()) {
                retval=e.assembleBuffer();
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


