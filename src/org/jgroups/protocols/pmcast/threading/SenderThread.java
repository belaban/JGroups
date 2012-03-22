package org.jgroups.protocols.pmcast.threading;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Protocol;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This thread is the responsible to send the group messages, i.e, create N messages and send N unicasts messages
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class SenderThread extends Thread {

    private boolean running = false;
    private Protocol groupMulticastProtocol;
    private Address localAddress;

    private final BlockingQueue<MessageToSend> sendingQueue;
    private final Log log = LogFactory.getLog(this.getClass());

    public SenderThread(Protocol protocol) {
        super("Group-Multicast-Sender-Thread");
        if (protocol == null) {
            throw new NullPointerException("Group Multicast Protocol can't be null");
        }
        this.groupMulticastProtocol = protocol;
        this.sendingQueue = new LinkedBlockingQueue<MessageToSend>();
    }

    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void addMessage(Message message, Set<Address> destination) throws InterruptedException {
        sendingQueue.put(new MessageToSend(message, destination));
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

                if (messageToSend.destination == null) {
                    groupMulticastProtocol.getDownProtocol().down(new Event(Event.MSG, messageToSend.message));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Send group message " + messageToSend.message + " to " + messageToSend.destination);
                    }
                    for (Address address : messageToSend.destination) {
                        if (address.equals(localAddress)) {
                            continue;
                        }
                        Message cpy = messageToSend.message.copy();
                        cpy.setDest(address);
                        groupMulticastProtocol.getDownProtocol().down(new Event(Event.MSG, cpy));
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

    private class MessageToSend {
        private Message message;
        private Set<Address> destination;

        private MessageToSend(Message message, Set<Address> destination) {
            this.message = message;
            this.destination = destination;
        }
    }
}
