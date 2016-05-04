package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
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

    /**
     * Sends all messages in the map. Messages for the same destination are bundled into a message list. The map will
     * be cleared when done
     */
    protected void sendBundledMessages() {
        if(log.isTraceEnabled()) {
            double percentage=100.0 / transport.getMaxBundleTimeout() * count;
            log.trace(BUNDLE_MSG, transport.localAddress(), numMessages(), count, percentage, msgs.size(), msgs.keySet());
        }

        for(Map.Entry<SingletonAddress,List<Message>> entry: msgs.entrySet()) {
            List<Message> list=entry.getValue();
            if(list.isEmpty())
                continue;

            if(list.size() == 1)
                sendSingleMessage(list.get(0));
            else {
                SingletonAddress dst=entry.getKey();
                sendMessageList(dst.getAddress(), list.get(0).getSrc(), dst.getClusterName(), list);
                if(transport.statsEnabled())
                    transport.incrBatchesSent();
            }
        }
        msgs.clear();
        count=0;
    }

    protected int numMessages() {
        int num=0;
        Collection<List<Message>> values=msgs.values();
        for(List<Message> list: values)
            num+=list.size();
        return num;
    }


    protected void sendSingleMessage(final Message msg) {
        Address dest=msg.getDest();
        try {
            output.position(0);
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
            output.position(0);
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
            tmp=new LinkedList<>();
            msgs.put(dest, tmp);
        }
        tmp.add(msg);
        count+=size;
    }
}
