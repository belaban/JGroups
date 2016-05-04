package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.AsciiString;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import static org.jgroups.protocols.TP.assertPositive;

/**
 * This bundler uses the same logic as {@link TP.TransferQueueBundler} but does not allocate
 * memory except for the buffer itself and does not use complex data structures.
 * @author Radim Vansa
 */
public class SimplifiedTransferQueueBundler extends TransferQueueBundler {
    protected static final int          MSG_BUF_SIZE=512;
    protected final Message[]           msg_queue=new Message[MSG_BUF_SIZE];
    protected int                       curr;

    protected SimplifiedTransferQueueBundler(int capacity) {
        super(new ArrayBlockingQueue<Message>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    protected void addMessage(Message msg, long size) {
        try {
            while(curr < MSG_BUF_SIZE && msg_queue[curr] != null) ++curr;
            if(curr < MSG_BUF_SIZE) {
                msg_queue[curr]=msg;
                ++curr;
            }
            else {
                sendBundledMessages();
                curr=0;
                msg_queue[0]=msg;
            }
        }
        finally {
            count+=size;
        }
    }

    protected void sendBundledMessages() {
        int start=0;
        for(;;) {
            for(; start < MSG_BUF_SIZE && msg_queue[start] == null; ++start) ;
            if(start >= MSG_BUF_SIZE) {
                count=0;
                return;
            }
            Address dest=msg_queue[start].getDest();
            byte[] clusterName=getMsgClusterName(msg_queue[start]);
            int numMsgs=1;
            if(transport.isSingleton()) {
                for(int i=start + 1; i < MSG_BUF_SIZE; ++i) {
                    Message msg=msg_queue[i];
                    if(msg != null && (dest == msg.getDest() || (dest != null && dest.equals(msg.getDest())))
                      && Arrays.equals(clusterName, getMsgClusterName(msg))) {
                        msg.setDest(dest); // avoid further equals() calls
                        numMsgs++;
                    }
                }
            }
            else {
                for(int i=start + 1; i < MSG_BUF_SIZE; ++i) {
                    Message msg=msg_queue[i];
                    if(msg != null && (dest == msg.getDest() || (dest != null && dest.equals(msg.getDest())))) {
                        msg.setDest(dest); // avoid further equals() calls
                        numMsgs++;
                    }
                }
            }
            try {
                output.position(0);
                if(numMsgs == 1) {
                    sendSingleMessage(msg_queue[start], output);
                    msg_queue[start]=null;
                }
                else {
                    Util.writeMessageListHeader(dest, msg_queue[start].getSrc(), clusterName, numMsgs, output, dest == null);
                    for(int i=start; i < MSG_BUF_SIZE; ++i) {
                        Message msg=msg_queue[i];
                        // since we assigned the matching destination we can do plain ==
                        if(msg != null && msg.getDest() == dest) {
                            msg.writeToNoAddrs(msg.getSrc(), output, transport.getId());
                            msg_queue[i]=null;
                        }
                    }
                    transport.doSend(transport.isSingleton()? new AsciiString(clusterName) : null, output.buffer(), 0, output.position(), dest);
                }
                start++;
            }
            catch(Exception e) {
                log.error("Failed to send message", e);
            }
        }
    }

    private byte[] getMsgClusterName(Message msg) {
        return ((TpHeader) msg.getHeader(transport.getId())).cluster_name;
    }

    protected void sendSingleMessage(Message msg, ByteArrayDataOutputStream output) {
        Address dest = msg.getDest();
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
}

