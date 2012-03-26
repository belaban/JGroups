package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This thread is responsible for sending the group messages, i.e, create N messages and send N unicasts messages
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class SenderThread extends Thread {

    private volatile boolean running = false;
    private Protocol toa;
    private Address  localAddress;

    private final BlockingQueue<MessageToSend> sendingQueue;
    private final Log log = LogFactory.getLog(this.getClass());

    public SenderThread(Protocol protocol) {
        super("TOA-Sender-Thread");
        if (protocol == null) {
            throw new NullPointerException("TOA protocol can't be null");
        }
        this.toa= protocol;
        this.sendingQueue = new LinkedBlockingQueue<MessageToSend>();
    }

    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void addMessage(Message message, Collection<Address> destinations) throws InterruptedException {
        sendingQueue.put(new MessageToSend(message, destinations));
    }

    public void clear() {
        sendingQueue.clear();
    }

    @Deprecated
    public void addUnicastMessage(Message message) throws InterruptedException {
        sendingQueue.put(new MessageToSend(message, null));
    }

    @Override
    public void start() {
        running = true;
        super.start();
    }

    @Override
    public void run() {
        while (running) {
            try {
                MessageToSend messageToSend = sendingQueue.take();

                if (messageToSend.destinations == null) {
                    toa.getDownProtocol().down(new Event(Event.MSG, messageToSend.message));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Send anycast total order message " + messageToSend.message + " to " + messageToSend.destinations);
                    }
                    for (Address address : messageToSend.destinations) {
                        if (address.equals(localAddress)) {
                            continue;
                        }
                        Message cpy = messageToSend.message.copy();
                        cpy.setDest(address);
                        toa.getDownProtocol().down(new Event(Event.MSG, cpy));
                    }
                }
            } catch (InterruptedException e) {
                //interrupted
            }
        }
    }

    @Override
    public void interrupt() {
        running = false;
        super.interrupt();
    }

    private static class MessageToSend {
        private final Message             message;
        private final Collection<Address> destinations;

        private MessageToSend(Message message, Collection<Address> destinations) {
            this.message = message;
            this.destinations= destinations;
        }
    }
}
