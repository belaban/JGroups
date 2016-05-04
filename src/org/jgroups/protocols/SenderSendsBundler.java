package org.jgroups.protocols;

import org.jgroups.Message;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 */
public class SenderSendsBundler extends BaseBundler {
    protected final AtomicInteger num_senders=new AtomicInteger(0); // current senders adding msgs to the bundler

    public void send(Message msg) throws Exception {
        long size=msg.size();
        num_senders.incrementAndGet();

        lock.lock();
        try {
            num_senders.decrementAndGet();

            if(count + size >= transport.getMaxBundleSize())
                sendBundledMessages();

            // at this point, we haven't sent our message yet !
            if(num_senders.get() == 0) { // no other sender threads present at this time
                if(count == 0)
                    sendSingleMessage(msg);
                else {
                    addMessage(msg,size);
                    sendBundledMessages();
                }
            }
            else  // there are other sender threads waiting, so our message will be sent by a different thread
                addMessage(msg, size);
        }
        finally {
            lock.unlock();
        }
    }
}
