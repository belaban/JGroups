package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a message batch; multiple messages from the same sender to the same receiver(s). This class is unsynchronized.
 * @author Bela Ban
 * @since  3.3
 */
public class MessageBatch implements Iterable<Message> {

    /** The destination address. Null if this is a multicast message batch, non-null if the batch is sent to a specific member */
    protected Address          dest;

    /** The sender of the message batch */
    protected Address          sender;

    /** The name of the cluster in which the message batch is sent, this is equivalent to TpHeader.cluster_name */
    protected AsciiString      cluster_name;

    /** The storage of the messages; removed messages have a null element */
    protected Message[]        messages;

    /** Index of the next message to be inserted */
    protected int              index;

    /** Whether all messages have dest == null (multicast) or not */
    protected boolean          multicast;

    /** Whether this message batch contains only OOB messages, or only regular messages */
    protected Mode             mode=Mode.REG;

    protected static final int INCR=5; // number of elements to add when resizing
    protected static final     Visitor<Integer> length_visitor=(msg, batch) -> msg != null? msg.getLength() : 0;
    protected static final     Visitor<Long>    total_size_visitor=(msg, batch) -> msg != null? msg.size() : 0;


    public MessageBatch(int capacity) {
        this.messages=new Message[capacity];
    }

    public MessageBatch(Collection<Message> msgs) {
        messages=new Message[msgs.size()];
        for(Message msg: msgs)
            messages[index++]=msg;
        mode=determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Collection<Message> msgs) {
        this(dest, sender, cluster_name, multicast, msgs, null);
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast,
                        Collection<Message> msgs, Predicate<Message> filter) {
        messages=new Message[msgs.size()];
        for(Message msg: msgs) {
            if(filter != null && !filter.test(msg))
                continue;
            messages[index++]=msg;
        }
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        this.mode=determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Mode mode, int capacity) {
        this(capacity);
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        this.mode=mode;
    }

    public Address      dest()                        {return dest;}
    public MessageBatch dest(Address dest)            {this.dest=dest; return this;}
    public Address      sender()                      {return sender;}
    public MessageBatch sender(Address sender)        {this.sender=sender; return this;}
    public AsciiString  clusterName()                 {return cluster_name;}
    public MessageBatch clusterName(AsciiString name) {this.cluster_name=name; return this;}
    public boolean      multicast()                   {return multicast;}
    public Mode         mode()                        {return mode;}
    public MessageBatch mode(Mode mode)               {this.mode=mode; return this;}
    public int          capacity()                    {return messages.length;}


    /** Returns the underlying message array. This is only intended for testing ! */
    public Message[]    array() {
        return messages;
    }

    public Message first() {
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                return messages[i];
        return null;
    }

    public Message last() {
        for(int i=index -1; i >= 0; i--)
            if(messages[i] != null)
                return messages[i];
        return null;
    }

    public MessageBatch add(final Message msg) {
        if(msg == null) return this;
        if(index >= messages.length)
            resize();
        messages[index++]=msg;
        return this;
    }

    /**
     * Replaces a message in the batch with another one
     * @param existing_msg The message to be replaced. The message has to be non-null and is found by identity (==)
     *                     comparison
     * @param new_msg The message to replace the existing message with, can be null
     * @return
     */
    public MessageBatch replace(Message existing_msg, Message new_msg) {
        if(existing_msg == null)
            return this;
        for(int i=0; i < index; i++) {
            if(messages[i] != null && messages[i] == existing_msg) {
                messages[i]=new_msg;
                break;
            }
        }
        return this;
    }

    /**
     * Replaces all messages which match a given filter with a replacement message
     * @param filter the filter. If null, no changes take place. Note that filter needs to be able to handle null msgs
     * @param replacement the replacement message. Can be null, which essentially removes all messages matching filter
     * @param match_all whether to replace the first or all matches
     * @return the MessageBatch
     */
    public MessageBatch replace(Predicate<Message> filter, Message replacement, boolean match_all) {
        if(filter == null)
            return this;
        for(int i=0; i < index; i++) {
            if(filter.test(messages[i])) {
                messages[i]=replacement;
                if(!match_all)
                    break;
            }
        }
        return this;
    }

    /**
     * Removes the current message (found by indentity (==)) by nulling it in the message array
     * @param msg
     * @return
     */
    public MessageBatch remove(Message msg) {
        return replace(msg, null);
    }

    /**
     * Removes all messages which match filter
     * @param filter the filter. If null, no removal takes place
     * @return the MessageBatch
     */
    public MessageBatch remove(Predicate<Message> filter) {
        return replace(filter, null, true);
    }

    public MessageBatch clear() {
        for(int i=0; i < index; i++)
            messages[i]=null;
        index=0;
        return this;
    }

    /** Removes and returns all messages which have a header with ID == id */
    public Collection<Message> getMatchingMessages(final short id, boolean remove) {
        return map((msg, batch) -> {
            if(msg != null && msg.getHeader(id) != null) {
                if(remove)
                    batch.remove(msg);
                return msg;
            }
            return null;
        });
    }


    /** Applies a function to all messages and returns a list of the function results */
    public <T> Collection<T> map(Visitor<T> visitor) {
        Collection<T> retval=null;
        for(int i=0; i < index; i++) {
            try {
                T result=visitor.visit(messages[i], this);
                if(result != null) {
                    if(retval == null)
                        retval=new ArrayList<>();
                    retval.add(result);
                }
            }
            catch(Throwable t) {
            }
        }
        return retval;
    }


    /** Returns the number of non-null messages */
    public int size() {
        int retval=0;
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                retval++;
        return retval;
    }

    public boolean isEmpty() {
        for(int i=0; i < index; i++)
            if(messages[i] != null)
                return false;
        return true;
    }

    public Mode determineMode() {
        int num_oob=0, num_reg=0, num_internal=0;
        for(int i=0; i < index; i++) {
            if(messages[i] == null)
                continue;
            if(messages[i].isFlagSet(Message.Flag.OOB))
                num_oob++;
            else if(messages[i].isFlagSet(Message.Flag.INTERNAL))
                num_internal++;
            else
                num_reg++;
        }
        if(num_internal > 0 && num_oob == 0 && num_reg == 0)
            return Mode.INTERNAL;
        if(num_oob > 0 && num_internal == 0 && num_reg == 0)
            return Mode.OOB;
        if(num_reg > 0 && num_oob == 0 && num_internal == 0)
            return Mode.REG;
        return Mode.MIXED;
    }


    /** Returns the size of the message batch (by calling {@link org.jgroups.Message#size()} on all messages) */
    public long totalSize() {
        long retval=0;
        for(int i=0; i < index; i++)
            retval+=total_size_visitor.visit(messages[i], this);
        return retval;
    }

    /** Returns the total number of bytes of the message batch (by calling {@link org.jgroups.Message#getLength()} on all messages) */
    public int length() {
        int retval=0;
        for(int i=0; i < index; i++)
            retval+=length_visitor.visit(messages[i], this);
        return retval;
    }


    /** Iterator which iterates only over non-null messages, skipping null messages */
    public Iterator<Message> iterator() {
        return new BatchIterator(index);
    }

    public Stream<Message> stream() {
        Spliterator<Message> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        sb.append(", mode=" + mode);
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(sb.length() > 0)
            sb.append(", ");
        sb.append(size() + " messages [capacity=" + messages.length + "]");

        return sb.toString();
    }

    public String printHeaders() {
        StringBuilder sb=new StringBuilder().append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        sb.append("\n").append(size()).append(":\n");
        int count=1;
        for(Message msg: this)
            sb.append("#").append(count++).append(": ").append(msg.printHeaders()).append("\n");
        return sb.toString();
    }

    protected void resize() {
        Message[] tmp=new Message[messages.length + INCR];
        System.arraycopy(messages,0,tmp,0,messages.length);
        messages=tmp;
    }


    /** Used for iteration over the messages */
    @FunctionalInterface
    public interface Visitor<T> {
        /**
         * Called when iterating over the message batch
         * @param msg The message, can be null
         * @param batch
         * @return
         */
        T visit(final Message msg, final MessageBatch batch);
    }

    public enum Mode {OOB, REG, INTERNAL, MIXED}


    /** Iterates over <em>non-null</em> elements of a batch, skipping null elements */
    protected class BatchIterator implements Iterator<Message> {
        protected int       current_index=-1;
        protected final int saved_index; // index at creation time of the iterator

        public BatchIterator(int saved_index) {
            this.saved_index=saved_index;
        }

        public boolean hasNext() {
            // skip null elements
            while(current_index +1 < saved_index && messages[current_index+1] == null)
                current_index++;
            return current_index +1 < saved_index;
        }

        public Message next() {
            if(current_index +1 >= messages.length)
                throw new NoSuchElementException();
            return messages[++current_index];
        }

        public void remove() {
            if(current_index >= 0)
                messages[current_index]=null;
        }
    }


}
