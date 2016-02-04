package org.jgroups.protocols.tom;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The implementation of the Delivery Manager
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliveryManagerImpl implements DeliveryManager {
    private final SortedSet<MessageInfo> deliverySet = new TreeSet<>();
    private final ConcurrentMap<MessageID, MessageInfo> messageCache = new ConcurrentHashMap<>(8192, .75f, 64);
    private final SequenceNumberManager sequenceNumberManager = new SequenceNumberManager();

    public long addLocalMessageToDeliver(MessageID messageID, Message message, ToaHeader header) {
        MessageInfo messageInfo;
        long sequenceNumber;
        synchronized (deliverySet) {
            sequenceNumber = sequenceNumberManager.getAndIncrement();
            header.setSequencerNumber(sequenceNumber);
            messageInfo = new MessageInfo(messageID, message, sequenceNumber);
            deliverySet.add(messageInfo);
        }
        messageCache.put(messageID, messageInfo);
        return sequenceNumber;
    }

    public long addRemoteMessageToDeliver(MessageID messageID, Message message, long remoteSequenceNumber) {
        MessageInfo messageInfo;
        long sequenceNumber;
        synchronized (deliverySet) {
            sequenceNumber = sequenceNumberManager.updateAndGet(remoteSequenceNumber);
            messageInfo = new MessageInfo(messageID, message, sequenceNumber);
            deliverySet.add(messageInfo);
        }
        messageCache.put(messageID, messageInfo);
        return sequenceNumber;
    }

    public void updateSequenceNumber(long sequenceNumber) {
        synchronized (deliverySet) {
            sequenceNumberManager.update(sequenceNumber);
        }
    }

    /**
     * marks the message as ready to deliver and set the final sequence number (to be ordered)
     *
     * @param messageID           the message ID
     * @param finalSequenceNumber the final sequence number
     */
    public void markReadyToDeliver(MessageID messageID, long finalSequenceNumber) {
        markReadyToDeliverV2(messageID, finalSequenceNumber);
    }

    private void markReadyToDeliverV2(MessageID messageID, long finalSequenceNumber) {
        MessageInfo messageInfo = messageCache.remove(messageID);

        if (messageInfo == null) {
            throw new IllegalStateException("Message ID not found in to deliver list. this can't happen. " +
                    "Message ID is " + messageID);
        }

        boolean needsUpdatePosition = messageInfo.isUpdatePositionNeeded(finalSequenceNumber);

        synchronized (deliverySet) {
            sequenceNumberManager.update(finalSequenceNumber);
            if (needsUpdatePosition) {
                deliverySet.remove(messageInfo);
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
                deliverySet.add(messageInfo);
            } else {
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
            }

            if (deliverySet.first().isReadyToDeliver()) {
                deliverySet.notifyAll();
            }
        }
    }

    public final void removeLeavers(Collection<Address> leavers) {
        if (leavers == null) {
            return;
        }
        List<MessageInfo> toRemove = new LinkedList<>();
        synchronized (deliverySet) {
            deliverySet.stream()
              .filter(messageInfo -> leavers.contains(messageInfo.getMessage().getSrc()) && !messageInfo.isReadyToDeliver())
              .forEach(toRemove::add);

            deliverySet.removeAll(toRemove);
            if (!deliverySet.isEmpty() && deliverySet.first().isReadyToDeliver()) {
                deliverySet.notifyAll();
            }
        }
        for (MessageInfo removed : toRemove) {
            messageCache.remove(removed.messageID);
        }
    }

    //see the interface javadoc
    @Override
    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> toDeliver = new LinkedList<>();
        synchronized (deliverySet) {
            while (deliverySet.isEmpty() || !deliverySet.first().isReadyToDeliver()) {
                deliverySet.wait();
            }

            Iterator<MessageInfo> iterator = deliverySet.iterator();

            while (iterator.hasNext()) {
                MessageInfo messageInfo = iterator.next();
                if (messageInfo.isReadyToDeliver()) {
                    toDeliver.add(messageInfo.getMessage());
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        return toDeliver;
    }

    /**
     * remove all the pending messages
     */
    public void clear() {
        synchronized (deliverySet) {
            deliverySet.clear();
            messageCache.clear();
        }
    }

    /**
     * delivers a message that has only as destination member this node
     *
     * @param msg the message
     */
    public void deliverSingleDestinationMessage(Message msg, MessageID messageID) {
        synchronized (deliverySet) {
            long sequenceNumber = sequenceNumberManager.get();
            MessageInfo messageInfo = new MessageInfo(messageID, msg, sequenceNumber);
            messageInfo.updateAndmarkReadyToDeliver(sequenceNumber);
            deliverySet.add(messageInfo);
            if (deliverySet.first().isReadyToDeliver()) {
                deliverySet.notifyAll();
            }
        }
    }

    /**
     * Keeps the state of a message
     */
    private static class MessageInfo implements Comparable<MessageInfo> {

        private final MessageID messageID;
        private final Message message;
        private volatile long sequenceNumber;
        private volatile boolean readyToDeliver;

        public MessageInfo(MessageID messageID, Message message, long sequenceNumber) {
            if (messageID == null) {
                throw new NullPointerException("Message ID can't be null");
            }
            this.messageID = messageID;
            this.message = message.copy(true, true);
            this.sequenceNumber = sequenceNumber;
            this.readyToDeliver = false;
            this.message.setSrc(messageID.getAddress());
        }

        private Message getMessage() {
            return message;
        }

        private void updateAndmarkReadyToDeliver(long finalSequenceNumber) {
            this.readyToDeliver = true;
            this.sequenceNumber = finalSequenceNumber;
        }

        private boolean isReadyToDeliver() {
            return readyToDeliver;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null) {
                return false;
            }

            boolean isMessageID = o.getClass() == MessageID.class;

            if (o.getClass() != getClass() && !isMessageID) {
                return false;
            }

            if (isMessageID) {
                return messageID.equals(o);
            }

            MessageInfo that = (MessageInfo) o;

            return messageID.equals(that.messageID);
        }

        @Override
        public int hashCode() {
            return messageID.hashCode();
        }

        @Override
        public String toString() {
            return "MessageInfo{" +
                    "messageID=" + messageID +
                    ", sequenceNumber=" + sequenceNumber +
                    ", readyToDeliver=" + readyToDeliver +
                    '}';
        }

        public boolean isUpdatePositionNeeded(long finalSequenceNumber) {
            return sequenceNumber != finalSequenceNumber;
        }

        @Override
        public int compareTo(MessageInfo o) {
            if (o == null) {
                throw new NullPointerException();
            }
            int sameId = messageID.compareTo(o.messageID);
            if (sameId == 0) {
                return 0;
            }
            return sequenceNumber < o.sequenceNumber ? -1 : sequenceNumber == o.sequenceNumber ? sameId : 1;
        }
    }

    /**
     * It is used for testing (see the messages in JMX)
     *
     * @return unmodifiable set of messages
     */
    public Set<MessageInfo> getMessageSet() {
        synchronized (deliverySet) {
            return Collections.unmodifiableSet(deliverySet);
        }
    }
}
