package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This bundler uses the same logic as {@link TransferQueueBundler} but does not allocate
 * memory except for the buffer itself and does not use complex data structures.
 * @author Radim Vansa
 */
public class SimplifiedTransferQueueBundler extends TransferQueueBundler {
    protected static final int MSG_BUF_SIZE=512;
    protected final Message[]  msg_queue=new Message[MSG_BUF_SIZE];
    protected int              curr;

    public SimplifiedTransferQueueBundler() {
    }

    public SimplifiedTransferQueueBundler(int capacity) {
        super(new ArrayBlockingQueue<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public int size() {
        return curr + removeQueueSize();
    }

    protected void addMessage(Message msg, long size) {
        try {
            while(curr < MSG_BUF_SIZE && msg_queue[curr] != null) ++curr;
            if(curr < MSG_BUF_SIZE) {
                msg_queue[curr]=msg;
                ++curr;
            }
            else {
                sendBundledMessages(); // sets curr to 0
                msg_queue[0]=msg;
            }
        }
        finally {
            count+=size;
        }
    }

    protected void sendBundledMessages() {
        try {
            _sendBundledMessages();
        }
        finally {
            curr=0;
        }
    }

    protected void _sendBundledMessages() {
        int start=0;
        for(;;) {
            for(; start < MSG_BUF_SIZE && msg_queue[start] == null; ++start) ;
            if(start >= MSG_BUF_SIZE) {
                count=0;
                return;
            }
            Address dest=msg_queue[start].getDest();
            int numMsgs=1;
            for(int i=start + 1; i < MSG_BUF_SIZE; ++i) {
                Message msg=msg_queue[i];
                if(msg != null && (dest == msg.getDest() || (Objects.equals(dest, msg.getDest())))) {
                    msg.setDest(dest); // avoid further equals() calls
                    numMsgs++;
                }
            }
            try {
                output.position(0);
                if(numMsgs == 1) {
                    sendSingleMessage(msg_queue[start]);
                    msg_queue[start]=null;
                }
                else {
                    Util.writeMessageListHeader(dest, msg_queue[start].getSrc(), transport.cluster_name.chars(), numMsgs, output, dest == null);
                    for(int i=start; i < MSG_BUF_SIZE; ++i) {
                        Message msg=msg_queue[i];
                        // since we assigned the matching destination we can do plain ==
                        if(msg != null && msg.getDest() == dest) {
                            output.write(msg.getType());
                            msg.writeToNoAddrs(msg.getSrc(), output, transport.getId());
                            msg_queue[i]=null;
                        }
                    }
                    transport.doSend(output.buffer(), 0, output.position(), dest);
                }
                start++;
            }
            catch(Exception e) {
                log.error("Failed to send message", e);
            }
        }
    }


}
