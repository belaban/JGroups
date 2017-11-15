package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Implements storing of messages in a hashmap and sending of single messages and message batches. Most bundler
 * implementations will want to extend this class
 * @author Bela Ban
 * @since  4.0
 */
public abstract class BaseBundler implements Bundler {
    /** Keys are destinations, values are lists of Messages */
    protected final Map<Address,List<Message>>  msgs=new HashMap<>(24);
    protected TP                                transport;
    protected final ReentrantLock               lock=new ReentrantLock();
    protected @GuardedBy("lock") long           count;    // current number of bytes accumulated
    protected ByteArrayDataOutputStream         output;
    protected Log                               log;


    public void init(TP transport) {
        this.transport=transport;
        log=transport.getLog();
        output=new ByteArrayDataOutputStream(transport.getMaxBundleSize() + MSG_OVERHEAD);
    }
    public void start() {}
    public void stop()  {}
    public void send(Message msg) throws Exception {}

    public void viewChange(View view) {
        // code removed (https://issues.jboss.org/browse/JGRP-2324)
    }

    public int size() {
        lock.lock();
        try {
            return msgs.values().stream().flatMap(Collection::stream).map(Message::size).reduce(0, Integer::sum);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Sends all messages in the map. Messages for the same destination are bundled into a message list.
     * The map will be cleared when done.
     */
    @GuardedBy("lock") protected void sendBundledMessages() {
        for(Map.Entry<Address,List<Message>> entry: msgs.entrySet()) {
            List<Message> list=entry.getValue();
            if(list.isEmpty())
                continue;

            output.position(0);
            if(list.size() == 1)
                sendSingleMessage(list.get(0));
            else {
                Address dst=entry.getKey();
                sendMessageList(dst, list.get(0).getSrc(), list);
                if(transport.statsEnabled())
                    transport.incrBatchesSent(1);
            }
        }
        clearMessages();
        count=0;
    }

    @GuardedBy("lock") protected void clearMessages() {
        msgs.values().stream().filter(Objects::nonNull).forEach(List::clear);
    }


    protected void sendSingleMessage(final Message msg) {
        Address dest=msg.getDest();
        try {
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.incrNumSingleMsgsSent(1);
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("SendFailure"),
                      transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }



    protected void sendMessageList(final Address dest, final Address src, final List<Message> list) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, output, dest == null, transport.getId());
            transport.doSend(output.buffer(), 0, output.position(), dest);
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(), e);
        }
    }

    @GuardedBy("lock") protected void addMessage(Message msg, int size) {
        Address dest=msg.getDest();
        List<Message> tmp=msgs.computeIfAbsent(dest, k -> new ArrayList<>(5));
        tmp.add(msg);
        count+=size;
    }
}