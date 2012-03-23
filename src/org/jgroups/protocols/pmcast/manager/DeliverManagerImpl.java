package org.jgroups.protocols.pmcast.manager;

import org.jgroups.Message;
import org.jgroups.protocols.pmcast.MessageID;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The implementation of the Deliver Manager
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliverManagerImpl implements DeliverManager {
    private static final MessageInfoComparator COMPARATOR = new MessageInfoComparator();
    private final SortedSet<MessageInfo> toDeliverSet = new TreeSet<MessageInfo>(COMPARATOR);
    private final ConcurrentMap<MessageID, MessageInfo> messageCache = new ConcurrentHashMap<MessageID, MessageInfo>(8192, .75f, 64);

    /**
     * Add a new group message to be deliver 
     * @param messageID         the message ID
     * @param message           the message (needed to be deliver later)
     * @param sequenceNumber    the initial sequence number
     */
    public void addNewMessageToDeliver(MessageID messageID, Message message, long sequenceNumber) {
        MessageInfo messageInfo = new MessageInfo(messageID, message, sequenceNumber);
        synchronized (toDeliverSet) {
            toDeliverSet.add(messageInfo);
        }
        messageCache.put(messageID, messageInfo);
    }

    /**
     * marks the message as ready to deliver and set the final sequence number (to be ordered)
     * @param messageID             the message ID
     * @param finalSequenceNumber   the final sequence number
     */
    public void markReadyToDeliver(MessageID messageID, long finalSequenceNumber) {
        markReadyToDeliverV2(messageID, finalSequenceNumber);
    }

    @SuppressWarnings({"SuspiciousMethodCalls"})    
    private void markReadyToDeliverV1(MessageID messageID, long finalSequenceNumber) {
        //This is an old version. It was the bottleneck. Updated to version 2. It can be removed later
        synchronized (toDeliverSet) {
            MessageInfo messageInfo = null;
            boolean needsUpdatePosition = false;
            Iterator<MessageInfo> iterator = toDeliverSet.iterator();

            while (iterator.hasNext()) {
                MessageInfo aux = iterator.next();
                if (aux.equals(messageID)) {
                    messageInfo = aux;
                    if (messageInfo.sequenceNumber != finalSequenceNumber) {
                        needsUpdatePosition = true;
                        iterator.remove();
                    }
                    break;
                }
            }

            if (messageInfo == null) {
                throw new IllegalStateException("Message ID not found in to deliver list. this can't happen. " +
                        "Message ID is " + messageID);
            }
            messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
            if (needsUpdatePosition) {
                toDeliverSet.add(messageInfo);
            }

            if (!toDeliverSet.isEmpty() && toDeliverSet.first().isReadyToDeliver()) {
                toDeliverSet.notify();
            }
        }
    }

    private void markReadyToDeliverV2(MessageID messageID, long finalSequenceNumber) {
        MessageInfo messageInfo = messageCache.remove(messageID);

        if (messageInfo == null) {
            throw new IllegalStateException("Message ID not found in to deliver list. this can't happen. " +
                    "Message ID is " + messageID);
        }

        boolean needsUpdatePosition = messageInfo.isUpdatePositionNeeded(finalSequenceNumber);

        synchronized (toDeliverSet) {
            if (needsUpdatePosition) {
                toDeliverSet.remove(messageInfo);
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
                toDeliverSet.add(messageInfo);
            } else {
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
            }
            
            if (toDeliverSet.first().isReadyToDeliver()) {
                toDeliverSet.notify();
            }
        }
    }

    //see the interface javadoc
    @Override
    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> toDeliver = new LinkedList<Message>();
        synchronized (toDeliverSet) {
            while (toDeliverSet.isEmpty()) {
                toDeliverSet.wait();
            }

            if (!toDeliverSet.first().isReadyToDeliver()) {
                toDeliverSet.wait();
            }

            Iterator<MessageInfo> iterator = toDeliverSet.iterator();

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
        synchronized (toDeliverSet) {
            toDeliverSet.clear();
            messageCache.clear();
        }
    }

    /**
     * Keeps the state of a message
     */
    private static class MessageInfo {

        private MessageID messageID;
        private Message message;
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
    }

    private static class MessageInfoComparator implements Comparator<MessageInfo> {

        @Override
        public int compare(MessageInfo messageInfo, MessageInfo messageInfo1) {
            if (messageInfo == null) {
                return messageInfo1 == null ? 0 : 1;
            } else if (messageInfo1 == null) {
                return -1;
            }

            int compareMessageID = messageInfo.messageID.compareTo(messageInfo1.messageID);

            if (compareMessageID == 0) {
                return 0;
            }

            if (messageInfo.sequenceNumber != messageInfo1.sequenceNumber) {
                return Long.signum(messageInfo.sequenceNumber - messageInfo1.sequenceNumber);
            }

            return compareMessageID;
        }
    }

    /**
     * It is used for testing (see the messages in JMX)
     * @return unmodifiable set of messages
     */
    public Set<MessageInfo> getMessageSet() {
        synchronized (toDeliverSet) {
            return Collections.unmodifiableSet(toDeliverSet);
        }
    }
}
