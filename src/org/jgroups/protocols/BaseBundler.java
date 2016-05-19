package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.net.SocketException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.jgroups.protocols.TP.BUNDLE_MSG;
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
        lock.lock();
        try {
            for(Iterator<Address> it=msgs.keySet().iterator(); it.hasNext();) {
                Address mbr=it.next();
                if(mbr != null && !view.containsMember(mbr)) // skip dest == null
                    it.remove();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public int size() {
        int num=0;
        Collection<List<Message>> values=msgs.values();
        for(List<Message> list: values)
            num+=list.size();
        return num;
    }

    /**
     * Sends all messages in the map. Messages for the same destination are bundled into a message list.
     * The map will be cleared when done.
     */
    protected void sendBundledMessages() {
        if(log.isTraceEnabled()) {
            double percentage=100.0 / transport.getMaxBundleSize() * count;
            log.trace(BUNDLE_MSG, transport.localAddress(), size(), count, percentage, msgs.size(), msgs.keySet());
        }

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
                    transport.num_batches_sent++;
            }
        }
        clearMessages();
        count=0;
    }

    @GuardedBy("lock") protected void clearMessages() {
        msgs.values().stream().filter(list -> list != null).forEach(List::clear);
    }


    protected void sendSingleMessage(final Message msg) {
        Address dest=msg.getDest();
        try {
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.num_single_msgs_sent_instead_of_batch++;
        }
        catch(SocketException sock_ex) {
            log.trace(Util.getMessage("SendFailure"),
                      transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), sock_ex.toString(), msg.printHeaders());
        }
        catch(Throwable e) {
            log.error(Util.getMessage("SendFailure"),
                      transport.localAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }



    protected void sendMessageList(final Address dest, final Address src, final List<Message> list) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, output, dest == null, transport.getId());
            transport.doSend(output.buffer(), 0, output.position(), dest);
        }
        catch(SocketException sock_ex) {
            log.debug(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(),sock_ex);
        }
        catch(Throwable e) {
            log.error(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(), e);
        }
    }

    @GuardedBy("lock") protected void addMessage(Message msg, long size) {
        Address dest=msg.getDest();
        List<Message> tmp=msgs.get(dest);
        if(tmp == null) {
            tmp=new ArrayList<>(5);
            msgs.put(dest, tmp);
        }
        tmp.add(msg);
        count+=size;
    }
}