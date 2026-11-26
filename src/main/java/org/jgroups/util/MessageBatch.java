package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents a message batch; multiple messages from the same sender to the same receiver(s). This class is unsynchronized.
 * @author Bela Ban
 * @since  3.3
 */
public class MessageBatch implements Iterable<Message> {
    public enum Mode {OOB, REG}

    /** The destination address. Null if this is a multicast message batch, non-null if the batch is sent to a specific member */
    protected Address            dest;

    /** The sender of the message batch */
    protected Address            sender;

    /** The name of the cluster in which the message batch is sent, this is equivalent to TpHeader.cluster_name */
    protected AsciiString        cluster_name;

    /** The storage of the messages; removed messages have a null element */
    protected FastArray<Message> messages;

    /** Whether all messages have dest == null (multicast) or not */
    protected boolean            multicast;

    /** Whether this message batch contains only OOB messages, or only regular messages */
    protected Mode               mode;

    /** For benchmarking; may get removed without notice */
    protected long               timestamp; // ns


    public MessageBatch() {}

    public MessageBatch(int capacity) {
        this.messages=new FastArray<>(capacity);
    }

    public MessageBatch(Collection<Message> msgs) {
        messages=new FastArray<>(msgs.size());
        messages.addAll(msgs); // todo: check that no resize occurs!
        determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Collection<Message> msgs) {
        messages=new FastArray<>(msgs.size());
        messages.addAll(msgs);
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        determineMode();
    }

    public MessageBatch(Address dest, Address sender, AsciiString cluster_name, boolean multicast, Mode mode, int capacity) {
        this(capacity);
        this.dest=dest;
        this.sender=sender;
        this.cluster_name=cluster_name;
        this.multicast=multicast;
        this.mode=mode;
    }

    public Address      getDest()                        {return dest;}
    public Address      dest()                           {return dest;}
    public MessageBatch setDest(Address dest)            {this.dest=dest; return this;}
    public MessageBatch dest(Address dest)               {this.dest=dest; return this;}
    public Address      getSender()                      {return sender;}
    public Address      sender()                         {return sender;}
    public MessageBatch setSender(Address sender)        {this.sender=sender; return this;}
    public MessageBatch sender(Address sender)           {this.sender=sender; return this;}
    public AsciiString  getClusterName()                 {return cluster_name;}
    public AsciiString  clusterName()                    {return cluster_name;}
    public MessageBatch setClusterName(AsciiString name) {this.cluster_name=name; return this;}
    public MessageBatch clusterName(AsciiString name)    {this.cluster_name=name; return this;}
    public MessageBatch cluster(AsciiString name)        {this.cluster_name=name; return this;}
    public boolean      isMulticast()                    {return multicast;}
    public boolean      multicast()                      {return multicast;}
    public MessageBatch multicast(boolean flag)          {multicast=flag; return this;}
    public MessageBatch mcast(boolean flag)              {multicast=flag; return this;}
    public Mode         getMode()                        {return mode();}
    public Mode         mode()                           {return mode;}
    public MessageBatch setMode(Mode m)                  {return mode(m);}
    public MessageBatch mode(Mode m)                     {if(mode == null) mode=m; return this;}
    public int          capacity()                       {return messages.capacity();}
    public long         timestamp()                      {return timestamp;}
    public MessageBatch timestamp(long ts)               {timestamp=ts; return this;}
    public MessageBatch increment(int i)                 {messages.increment(i); return this;}
    public MessageBatch incr(int i)                      {return increment(i);}


    /** Returns the underlying message array. This is only intended for testing ! */
    public FastArray<Message> array() {
        return messages;
    }

    public <T extends Message> T first() {
        Iterator<Message> it=iterator();
        return it.hasNext()? (T)it.next() : null;
    }

    // not very efficient, but this is only used inside a trace log statement
    public <T extends Message> T last() {
        Iterator<Message> it=iterator();
        Message last=null;
        while(it.hasNext())
            last=it.next();
        return (T)last;
    }

    public MessageBatch add(final Message msg) {
        add(msg, true, true);
        return this;
    }

    /** Adds a message to the table
     * @param msg the message
     * @param resize whether or not to resize the table
     * @return always true if resize==true, else true if the message was added or false if not
     */
    public MessageBatch add(final Message msg, boolean resize) {
        return add(msg, resize, true);
    }

    /** Adds a message to the table
     * @param msg the message
     * @param resize whether or not to resize the table
     * @return always true if resize==true, else true if the message was added or false if not
     */
    public MessageBatch add(final Message msg, boolean resize, boolean determine_mode) {
        boolean added=messages.add(msg, resize);
        if(added && determine_mode)
            determineMode();
        return this;
    }

    public MessageBatch add(final MessageBatch batch) {
        return add(batch, true);
    }

    /**
     * Adds another batch to this one
     * @param batch the batch to add to this batch
     * @param resize when true, this batch will be resized to accommodate the other batch
     * @return the number of messages from the other batch that were added successfully. Will always be batch.size()
     * unless resize is false: in this case, the number of messages that were added successfully is returned
     */
    public MessageBatch add(final MessageBatch batch, boolean resize) {
        if(batch == null) return this;
        if(this == batch)
            throw new IllegalArgumentException("cannot add batch to itself");
        boolean added=messages.addAll(batch.array(), resize);
        if(added)
            determineMode();
        return this;
    }

    /**
     * Adds message to this batch from a message array
     * @param msgs  the message array
     * @param num_msgs the number of messages to add, should be {@literal <=} msgs.length
     */
    public MessageBatch add(Message[] msgs, int num_msgs) {
        boolean added=messages.addAll(msgs, num_msgs);
        if(added)
            determineMode();
        return this;
    }

    public MessageBatch add(Collection<Message> msgs) {
        boolean added=messages.addAll(msgs);
        if(added)
            determineMode();
        return this;
    }

    public MessageBatch set(Address dest, Address sender, Message[] msgs) {
        this.messages.set(msgs);
        this.dest=dest;
        this.sender=sender;
        determineMode();
        return this;
    }

    public MessageBatch removeIf(Predicate<Message> filter, boolean match_all) {
        messages.removeIf(filter, match_all);
        return this;
    }

    /**
     * Transfers messages from other to this batch. Optionally clears the other batch after the transfer
     * @param other the other batch
     * @param clear If true, the transferred messages are removed from the other batch
     * @return the number of transferred messages (may be 0 if the other batch was empty)
     */
    public int transferFrom(MessageBatch other, boolean clear) {
        if(other == null || this == other)
            return 0;
        int num=messages.transferFrom(other.messages, clear);
        if(num > 0)
            determineMode();
        return num;
    }

    public MessageBatch clear() {
        messages.clear(true);
        return this;
    }

    public MessageBatch reset() {
        messages.clear(false);
        return this;
    }

    public boolean anyMatch(Predicate<Message> pred) {
        return messages.anyMatch(pred);
    }

    public MessageBatch determineMode() {
        if(mode != null || messages.isEmpty())
            return this;
        Message first=messages.get(0);
        return mode(first.isFlagSet(Message.Flag.OOB)? Mode.OOB : Mode.REG);
    }

    /** Returns the number of non-null messages */
    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /** Returns the size of the message batch (by calling {@link Message#size()} on all messages) */
    public long totalSize() {
        long retval=0;
        for(Message msg: messages)
            retval+=msg.size();
        return retval;
    }

    /** Returns the total number of bytes of the message batch (by calling {@link Message#getLength()} on all messages) */
    public int length() {
        int retval=0;
        for(Message msg: messages)
            retval+=msg.getLength();
        return retval;
    }

    public MessageBatch resize(int new_capacity) {
        messages.resize(new_capacity);
        return this;
    }

    /** Iterator which iterates only over non-null messages, skipping null messages */
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    /** Iterates over all non-null message which match filter */
    public Iterator<Message> iterator(Predicate<Message> filter) {
        return messages.iterator(filter);
    }

    public Stream<Message> stream() {
        return stream(null, false);
    }

    public Stream<Message> stream(Predicate<Message> p) {
        return stream(p, false);
    }

    public Stream<Message> stream(Predicate<Message> p, boolean parallel) {
        Spliterator<Message> sp=Spliterators.spliterator(iterator(p), size(), 0);
        return StreamSupport.stream(sp, parallel);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("dest=" + dest);
        if(sender != null)
            sb.append(", sender=").append(sender);
        if(mode != null)
            sb.append(", mode=" + mode);
        if(cluster_name != null)
            sb.append(", cluster=").append(cluster_name);
        if(!sb.isEmpty())
            sb.append(", ");
        sb.append(size() + " messages [capacity=" + messages.capacity() + "]");
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


}
