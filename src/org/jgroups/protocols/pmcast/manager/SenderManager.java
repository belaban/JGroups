package org.jgroups.protocols.pmcast.manager;

import org.jgroups.Address;
import org.jgroups.protocols.pmcast.MessageID;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps track of all sended messages, until the final sequence number is known
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
public class SenderManager {

    public static final long NOT_READY = -1;

    private final ConcurrentMap<MessageID, MessageInfo> sentMessages = new ConcurrentHashMap<MessageID, MessageInfo>();

    /**
     * Add a new message sent
     * @param messageID             the message ID
     * @param destination           the destination set
     * @param initialSequenceNumber the initial sequence number
     * @param deliverToMySelf       true if *this* member is in destination sent, false otherwise
     */
    public void addNewMessageToSent(MessageID messageID, Set<Address> destination, long initialSequenceNumber,
                                    boolean deliverToMySelf) {
        MessageInfo messageInfo = new MessageInfo(destination, initialSequenceNumber, deliverToMySelf);
        if (deliverToMySelf) {
            messageInfo.setProposeReceived(messageID.getAddress());
        }
        sentMessages.put(messageID, messageInfo);
    }

    /**
     * Add a propose from a member in destination set
     * @param messageID         the message ID
     * @param from              the originator of the propose
     * @param sequenceNumber    the proposed sequence number
     * @return NOT_READY if the final sequence number is not know, or the final sequence number
     */
    public long addPropose(MessageID messageID, Address from, long sequenceNumber) {
        MessageInfo messageInfo = sentMessages.get(messageID);
        if (messageInfo != null && messageInfo.addPropose(from, sequenceNumber)) {
            return messageInfo.getAndMarkFinalSent();
        }
        return NOT_READY;
    }

    /**
     * Mark the message as sent
     * @param messageID the message ID
     * @return  return true if *this* member is in destination set
     */
    public boolean markSent(MessageID messageID) {
        MessageInfo messageInfo =  sentMessages.remove(messageID);
        return messageInfo != null && messageInfo.toSelfDeliver;
    }

    /**
     * obtains the destination set of a message
     * @param messageID the message ID
     * @return the destination set
     */
    public Set<Address> getDestination(MessageID messageID) {
        MessageInfo messageInfo = sentMessages.get(messageID);
        Set<Address> destination;
        if (messageInfo != null) {
            destination = new HashSet<Address>(messageInfo.destination);
        } else {
            destination = Collections.emptySet();
        }
        return destination;
    }

    /**
     * removes all pending messages
     */
    public void clear() {
        sentMessages.clear();
    }

    /**
     * The state of a message (destination, proposes missing, the highest sequence number proposed, etc...)
     */
    private static class MessageInfo {
        private ArrayList<Address> destination;
        private long highestSequenceNumberReceived;
        private BitSet receivedPropose;
        private boolean finalMessageSent = false;
        private boolean toSelfDeliver = false;

        private MessageInfo(Set<Address> addresses, long sequenceNumber, boolean selfDeliver) {
            this.destination = new ArrayList<Address>(addresses);
            this.highestSequenceNumberReceived = sequenceNumber;
            createNewBitSet(addresses.size());
            this.toSelfDeliver = selfDeliver;
        }

        private synchronized boolean addPropose(Address from, long sequenceNumber) {
            setProposeReceived(from);
            highestSequenceNumberReceived = Math.max(highestSequenceNumberReceived, sequenceNumber);
            return checkAllProposesReceived();
        }

        private synchronized long getAndMarkFinalSent() {
            if (checkAllProposesReceived() && !finalMessageSent) {
                finalMessageSent = true;
                return highestSequenceNumberReceived;
            }
            return NOT_READY;
        }

        private void createNewBitSet(int maxElements) {
            receivedPropose = new BitSet(maxElements);
            for (int i = 0; i < maxElements; ++i) {
                receivedPropose.set(i);
            }
        }

        private void setProposeReceived(Address address) {
            int idx = destination.indexOf(address);
            if (idx == -1) {
                throw new IllegalStateException("Address doesn't exists in destination list. Address is " + address);
            }
            receivedPropose.set(idx, false);
        }

        private boolean checkAllProposesReceived() {
            return receivedPropose.isEmpty();
        }
    }

}
