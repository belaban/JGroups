package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.util.AsciiString;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.SingletonAddress;
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
 */
public class BaseBundler implements Bundler {
    /** Keys are destinations, values are lists of Messages */
    protected final Map<SingletonAddress,List<Message>>  msgs=new HashMap<>(24);
    protected TP                                         transport;
    @GuardedBy("lock") protected long                    count;    // current number of bytes accumulated
    protected final ReentrantLock                        lock=new ReentrantLock();
    protected ByteArrayDataOutputStream                  output;
    protected Log                                        log;

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
            for(Iterator<SingletonAddress> it=msgs.keySet().iterator(); it.hasNext();) {
                Address mbr=it.next().getAddress();
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
        for(List<Message> list: values) {
            if(list != null)
                num+=list.size();
        }
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

        for(Map.Entry<SingletonAddress,List<Message>> entry: msgs.entrySet()) {
            List<Message> list=entry.getValue();
            if(list == null || list.isEmpty())
                continue;

            output.position(0);
            if(list.size() == 1)
                sendSingleMessage(list.get(0));
            else {
                SingletonAddress dst=entry.getKey();
                sendMessageList(dst.getAddress(), list.get(0).getSrc(), dst.getClusterName(), list);
                if(transport.statsEnabled())
                    transport.incrBatchesSent();
            }
        }
        clearMessages();
        count=0;
    }

    @GuardedBy("lock") protected void clearMessages() {
        for(List<Message> l: msgs.values()) {
            if(l != null)
                l.clear();
        }
    }


    protected void sendSingleMessage(final Message msg) {
        Address dest=msg.getDest();
        try {
            Util.writeMessage(msg, output, dest == null);
            transport.doSend(transport.getClusterName(msg), output.buffer(), 0, output.position(), dest);
            if(transport.statsEnabled())
                transport.incrSingleMsgsInsteadOfBatches();
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



    protected void sendMessageList(final Address dest, final Address src, final byte[] cluster_name,
                                   final List<Message> list) {
        try {
            Util.writeMessageList(dest, src, cluster_name, list, output, dest == null, transport.getId()); // flushes output stream when done
            transport.doSend(transport.isSingleton()? new AsciiString(cluster_name) : null, output.buffer(), 0, output.position(), dest);
        }
        catch(SocketException sock_ex) {
            log.debug(Util.getMessage("FailureSendingMsgBundle"),transport.localAddress(),sock_ex);
        }
        catch(Throwable e) {
            log.error(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(), e);
        }
    }

    @GuardedBy("lock") protected void addMessage(Message msg, long size) {
        byte[] cname=!transport.isSingleton()? transport.getClusterNameAscii().chars():
          ((TpHeader)msg.getHeader(transport.getId())).cluster_name;

        SingletonAddress dest=new SingletonAddress(cname, msg.getDest());
        List<Message> tmp=msgs.get(dest);
        if(tmp == null) {
            tmp=new ArrayList<>(5);
            msgs.put(dest, tmp);
        }
        tmp.add(msg);
        count+=size;
    }

    protected void checkForSharedTransport(TP tp) {
        if(tp.isSingleton())
            throw new IllegalStateException(String.format("bundler %s cannot handle shared transports",
                                                          getClass().getSimpleName()));
    }
}
