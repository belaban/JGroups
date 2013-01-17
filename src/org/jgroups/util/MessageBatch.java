package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

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

    /** The name of the cluster in which the message batch is sent, this is equivalent to TpHeader.channel_name */
    protected String           cluster_name;

    /** The storage of the messages; removed messages have a null element */
    protected Message[]        messages;

    /** Index of the next message to be inserted */
    protected int              index;

    /** Whether all messages have dest == null (multicast) or not */
    protected boolean          multicast;

    /** Whether this message batch contains only OOB messages, or only regular messages */
    protected Mode             mode;

    protected static final int INCR=5; // number of elements to add when resizing


    public MessageBatch(int capacity) {
        this.messages=new Message[capacity];
    }

    public MessageBatch(Collection<Message> msgs) {
        messages=new Message[msgs.size()];
        for(Message msg: msgs)
            messages[index++]=msg;
    }

    public MessageBatch(Address dest, Address sender, String cluster_name, boolean multicast, Mode mode, int capacity) {
        this(capacity);
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        this.mode=mode;
    }

    public Address      dest()                   {return dest;}
    public MessageBatch dest(Address dest)       {this.dest=dest; return this;}
    public Address      sender()                 {return sender;}
    public MessageBatch sender(Address sender)   {this.sender=sender; return this;}
    public String       clusterName()            {return cluster_name;}
    public MessageBatch clusterName(String name) {this.cluster_name=name; return this;}
    public boolean      multicast()              {return multicast;}
    public Mode         mode()                   {return mode;}
    public int          capacity()               {return messages.length;}


    public Message get(int index) {
        if(index >= 0 && index < messages.length)
            return messages[index];
        return null;
    }

    public MessageBatch set(int index, final Message msg) {
        if(index >= 0 && index < messages.length)
            messages[index]=msg;
        return this;
    }

    public MessageBatch add(final Message msg) {
        if(msg == null) return this;
        if(index >= messages.length)
            resize();
        messages[index++]=msg;
        return this;
    }

    public MessageBatch remove(int index) {
        if(index >= 0 && index < messages.length)
            messages[index]=null;
        return this;
    }

    public MessageBatch removeAll() {
        for(int i=0; i < messages.length; i++)
            messages[i]=null;
        index=0;
        return this;
    }

    /** Removes and returns all messages which have a header with ID == id */
    public Collection<Message> getMatchingMessages(final short id, final boolean remove) {
        return map(new Visitor<Message>() {
            public Message visit(int index, Message msg, MessageBatch batch) {
                if(msg != null && msg.getHeader(id) != null) {
                    if(remove)
                        batch.remove(index);
                    return msg;
                }
                return null;
            }
        });
    }


    /** Applies a function to all messages and returns a list of the function results */
    public <T> Collection<T> map(Visitor<T> visitor) {
        Collection<T> retval=null;
        for(int i=0; i < messages.length; i++) {
            T result=visitor.visit(i, messages[i], this);
            if(result != null) {
                if(retval == null)
                    retval=new ArrayList<T>();
                retval.add(result);
            }
        }
        return retval;
    }



    /** Returns the number of non-null messages */
    public int size() {
        int retval=0;
        Visitor<Integer> visitor=new Visitor<Integer>() {
            public Integer visit(int index, Message msg, MessageBatch batch) {
                return msg != null? 1 : 0;
            }
        };
        for(int i=0; i < messages.length; i++)
            retval+=visitor.visit(i, messages[i], this);
        return retval;
    }

    public boolean isEmpty() {
        for(Message msg: messages)
            if(msg != null)
                return false;
        return true;
    }


    /** Returns the size of the message batch (by calling {@link org.jgroups.Message#size()} on all messages) */
    public long totalSize() {
        long retval=0;
        Visitor<Long> visitor=new Visitor<Long>() {
            public Long visit(int index, Message msg, MessageBatch batch) {
                return msg != null? msg.size() : 0;
            }
        };
        for(int i=0; i < messages.length; i++)
            retval+=visitor.visit(i, messages[i], this);
        return retval;
    }

    /** Returns the total number of bytes of the message batch (by calling {@link org.jgroups.Message#getLength()} on all messages) */
    public int length() {
        int retval=0;
        Visitor<Integer> visitor=new Visitor<Integer>() {
            public Integer visit(int index, Message msg, MessageBatch batch) {
                return msg != null? msg.getLength() : 0;
            }
        };
        for(int i=0; i < messages.length; i++)
            retval+=visitor.visit(i, messages[i], this);
        return retval;
    }



    public Iterator<Message> iterator() {
        return new BatchIterator();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(sb.length() > 0)
            sb.append(", ");
        sb.append(size() + " messages [capacity=" + messages.length + "]");

        return sb.toString();
    }

    protected void resize() {
        Message[] tmp=new Message[messages.length + INCR];
        System.arraycopy(messages, 0, tmp, 0, messages.length);
        messages=tmp;
    }


    /** Used for iteration over the messages */
    public interface Visitor<T> {
        T visit(int index, final Message msg, final MessageBatch batch);
    }

    public enum Mode {OOB, REG, MIXED}


    protected class BatchIterator implements Iterator<Message> {
        protected int current_index=-1;

        public boolean hasNext() {
            return current_index +1 < messages.length;
        }

        public Message next() {
            if(current_index +1 >= messages.length)
                throw new NoSuchElementException();
            return messages[++current_index];
        }

        public void remove() {
            if(current_index >= 0)
                MessageBatch.this.remove(current_index);
        }
    }
}
