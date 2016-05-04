package org.jgroups.protocols;


import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.concurrent.TimeUnit;


/**
 * The sender's thread adds a message to the hashmap and - if the accumulated size has been exceeded - sends all
 * bundled messages. The cost of sending the bundled messages is therefore distributed over different threads;
 * whoever happens to send a message exceeding the max size gets to send the accumulated messages. We also use a
 * number of timer tasks to send bundled messages after a certain time has elapsed. This is necessary e.g. when a
 * message is added that doesn't exceed the max size, but then no further messages are added, so elapsed time
 * will trigger the sending, not exceeding of the max size.
 */
@Deprecated
public class SenderSendsWithTimerBundler extends BaseBundler implements Runnable {
    protected static final int MIN_NUMBER_OF_BUNDLING_TASKS=2;
    protected int              num_bundling_tasks=0;

    public void send(Message msg) throws Exception {
        long    size=msg.size();
        boolean do_schedule=false;

        lock.lock();
        try {
            if(count + size >= transport.getMaxBundleSize())
                sendBundledMessages();
            addMessage(msg, size);
            if(num_bundling_tasks < MIN_NUMBER_OF_BUNDLING_TASKS) {
                num_bundling_tasks++;
                do_schedule=true;
            }
        }
        finally {
            lock.unlock();
        }

        if(do_schedule)
            transport.getTimer().schedule(this, transport.getMaxBundleTimeout(), TimeUnit.MILLISECONDS);
    }

    public void run() {
        lock.lock();
        try {
            if(!msgs.isEmpty()) {
                try {
                    sendBundledMessages();
                }
                catch(Exception e) {
                    log.error(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(), e);
                }
            }
        }
        finally {
            num_bundling_tasks--;
            lock.unlock();
        }
    }

    public String toString() {return getClass() + ": BundlingTimer";}
}
