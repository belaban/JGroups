package org.jgroups.protocols.tom;

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
    private static final MessageInfoComparator COMPARATOR = new MessageInfoComparator();
    private final SortedSet<MessageInfo> deliverySet= new TreeSet<MessageInfo>(COMPARATOR);
    private final ConcurrentMap<MessageID, MessageInfo> messageCache = new ConcurrentHashMap<MessageID, MessageInfo>(8192, .75f, 64);
    private final Set<Message> singleDestinationSet = new HashSet<Message>();

    /**
     * Add a new group message to be deliver 
     * @param messageID         the message ID
     * @param message           the message (needed to be deliver later)
     * @param sequenceNumber    the initial sequence number
     */
    public void addNewMessageToDeliver(MessageID messageID, Message message, long sequenceNumber) {
        MessageInfo messageInfo = new MessageInfo(messageID, message, sequenceNumber);
        synchronized (deliverySet) {
            deliverySet.add(messageInfo);
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
        synchronized (deliverySet) {
            MessageInfo messageInfo = null;
            boolean needsUpdatePosition = false;
            Iterator<MessageInfo> iterator = deliverySet.iterator();

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
                deliverySet.add(messageInfo);
            }

            if (!deliverySet.isEmpty() && deliverySet.first().isReadyToDeliver()) {
                deliverySet.notify();
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

        synchronized (deliverySet) {
            if (needsUpdatePosition) {
                deliverySet.remove(messageInfo);
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
                deliverySet.add(messageInfo);
            } else {
                messageInfo.updateAndmarkReadyToDeliver(finalSequenceNumber);
            }
            
            if (deliverySet.first().isReadyToDeliver()) {
                deliverySet.notify();
            }
        }
    }

    //see the interface javadoc
    @Override
    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
        LinkedList<Message> toDeliver = new LinkedList<Message>();
        synchronized (deliverySet) {
            while (deliverySet.isEmpty() && singleDestinationSet.isEmpty()) {
                deliverySet.wait();
            }
            
            if (!singleDestinationSet.isEmpty()) {
                toDeliver.addAll(singleDestinationSet);
                singleDestinationSet.clear();
                return toDeliver;
            }

            if (!deliverySet.first().isReadyToDeliver()) {
                deliverySet.wait();
            }

           if (!singleDestinationSet.isEmpty()) {
                toDeliver.addAll(singleDestinationSet);
                singleDestinationSet.clear();                
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
    * @param msg  the message
    */
    public void deliverSingleDestinationMessage(Message msg) {
        synchronized (deliverySet) {
            singleDestinationSet.add(msg);
            deliverySet.notify();
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
        synchronized (deliverySet) {
            return Collections.unmodifiableSet(deliverySet);
        }
    }
}
