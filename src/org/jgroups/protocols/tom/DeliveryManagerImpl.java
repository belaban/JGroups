package org.jgroups.protocols.tom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;

/**
 * The implementation of the Delivery Manager
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class DeliveryManagerImpl implements DeliveryManager {
    @GuardedBy("deliverySet")
    private final SortedSet<MessageInfo> deliverySet = new TreeSet<>();
    private final ConcurrentMap<MessageID, MessageInfo> messageCache = new ConcurrentHashMap<>();
    @GuardedBy("deliverySet")
    private final SequenceNumberManager sequenceNumberManager = new SequenceNumberManager();
    @GuardedBy("deliverySet")
    private View currentView;

    /**
     * Updates the current view in use and returns a {@link Collection} with the members that left the cluster.
     */
    public final Collection<Address> handleView(View newView) {
        View oldView;
        synchronized (deliverySet) {
            oldView = getAndSetView(newView);
            deliverySet.removeIf(this::removeMessage);
            notifyIfNeeded();
        }
        return View.leftMembers(oldView, newView);
    }

    /**
     * @return the current view id or {@code -1} if no view is installed yet.
     */
    public long getViewId() {
        synchronized (deliverySet) {
            return internalGetViewId();
        }
    }

    /**
     * @return an unmodifiable collection with the current cluster members.
     */
    final Collection<Address> getViewMembers() {
        synchronized (deliverySet) {
            return currentView == null ? Collections.emptyList() : currentView.getMembers();
        }
    }

    long addRemoteMessageToDeliver(MessageID messageID, Message message, long remoteSequenceNumber, long viewId) {
        MessageInfo messageInfo;
        long sequenceNumber;
        synchronized (deliverySet) {
            long currentViewId = internalGetViewId();
            if (currentViewId != -1 //we have a view, check the view id
                && viewId <= currentViewId //message from the current or previous view, check the members
                && !currentView.containsMember(message.getSrc())) { //node no longer in view. discard it
                return -1;
            }
            sequenceNumber = sequenceNumberManager.updateAndGet(remoteSequenceNumber);
            messageInfo = new MessageInfo(messageID, message, sequenceNumber);
            deliverySet.add(messageInfo);
        }
        messageCache.put(messageID, messageInfo);
        return sequenceNumber;
    }

    long addLocalMessageToDeliver(MessageID messageID, Message message, ToaHeader header) {
        MessageInfo messageInfo;
        long sequenceNumber;
        synchronized (deliverySet) {
            sequenceNumber = sequenceNumberManager.getAndIncrement();
            messageInfo = new MessageInfo(messageID, message, sequenceNumber);
            deliverySet.add(messageInfo);
        }
        header.setSequencerNumber(sequenceNumber);
        messageCache.put(messageID, messageInfo);
        return sequenceNumber;
    }

    @GuardedBy("deliverSet")
    private long internalGetViewId() {
        return currentView == null ? -1 : currentView.getViewId().getId();
    }

    void updateSequenceNumber(long sequenceNumber) {
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
    void markReadyToDeliver(MessageID messageID, long finalSequenceNumber) {
        markReadyToDeliverV2(messageID, finalSequenceNumber);
    }

    /**
     * delivers a message that has only as destination member this node
     *
     * @param msg the message
     */
    void deliverSingleDestinationMessage(Message msg, MessageID messageID) {
        synchronized (deliverySet) {
            long sequenceNumber = sequenceNumberManager.get();
            MessageInfo messageInfo = new MessageInfo(messageID, msg, sequenceNumber);
            messageInfo.updateAndMarkReadyToDeliver(sequenceNumber);
            deliverySet.add(messageInfo);
            notifyIfNeeded();
        }
    }

    /**
     * It is used for testing (see the messages in JMX)
     *
     * @return unmodifiable set of messages
     */
    Set<MessageInfo> getMessageSet() {
        synchronized (deliverySet) {
            return Collections.unmodifiableSet(deliverySet);
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
            sequenceNumberManager.update(finalSequenceNumber);
            if (needsUpdatePosition) {
                deliverySet.remove(messageInfo);
                messageInfo.updateAndMarkReadyToDeliver(finalSequenceNumber);
                deliverySet.add(messageInfo);
            } else {
                messageInfo.updateAndMarkReadyToDeliver(finalSequenceNumber);
            }
            notifyIfNeeded();
        }
    }

    @GuardedBy("deliverySet")
    private View getAndSetView(View newView) {
        View oldView = currentView;
        currentView = newView;
        return oldView;
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

    public List<MessageInfo> getAllMessages() {
        synchronized (deliverySet) {
            return new ArrayList<>(deliverySet);
        }
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

    public SequenceNumberManager getSequenceNumberManager() {
        return sequenceNumberManager;
    }

    /**
     * @return {@code true} if the member who sent the message left the cluster and the message isn't ready to be deliver.
     */
    @GuardedBy("deliverySet")
    private boolean removeMessage(MessageInfo messageInfo) {
        if (currentView.containsMember(messageInfo.getMessage().getSrc()) || messageInfo.isReadyToDeliver()) {
            return false;
        } else {
            messageCache.remove(messageInfo.messageID);
            return true;
        }
    }

    @GuardedBy("deliverySet")
    private void notifyIfNeeded() {
        if (!deliverySet.isEmpty() && deliverySet.first().isReadyToDeliver()) {
            deliverySet.notify();
        }
    }

    /**
     * Keeps the state of a message
     */
    public static class MessageInfo implements Comparable<MessageInfo> {

        private final MessageID messageID;
        private final Message message;
        private volatile long sequenceNumber;
        private volatile boolean readyToDeliver;

        MessageInfo(MessageID messageID, Message message, long sequenceNumber) {
            if (messageID == null) {
                throw new NullPointerException("Message ID can't be null");
            }
            this.messageID = messageID;
            this.message = message.copy(true, true);
            this.sequenceNumber = sequenceNumber;
            this.readyToDeliver = false;
            this.message.setSrc(messageID.getAddress());
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        private Message getMessage() {
            return message;
        }

        private boolean isUpdatePositionNeeded(long finalSequenceNumber) {
            return sequenceNumber != finalSequenceNumber;
        }

        public boolean isReadyToDeliver() {
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

        private void updateAndMarkReadyToDeliver(long finalSequenceNumber) {
            this.readyToDeliver = true;
            this.sequenceNumber = finalSequenceNumber;
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
}
