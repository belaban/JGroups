package org.jgroups.protocols.pmcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.pmcast.header.GroupMulticastHeader;
import org.jgroups.protocols.pmcast.manager.DeliverManagerImpl;
import org.jgroups.protocols.pmcast.manager.SenderManager;
import org.jgroups.protocols.pmcast.manager.SequenceNumberManager;
import org.jgroups.protocols.pmcast.stats.StatsCollector;
import org.jgroups.protocols.pmcast.threading.DeliverThread;
import org.jgroups.protocols.pmcast.threading.SenderThread;
import org.jgroups.stack.Protocol;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**  
 * Total Order Multicast with three communication steps (based on Skeen's Algorithm)
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
@MBean(description = "Implementation of Total Order Multicast based on Skeen's Algorithm")
public class GROUP_MULTICAST extends Protocol implements DeliverProtocol {
    //managers
    private DeliverManagerImpl deliverManager;
    private SenderManager senderManager;

    //thread
    private final DeliverThread deliverThread;
    private final SenderThread multicastSenderThread;

    //local address
    private Address localAddress;

    //sequence numbers, messages ids and lock
    private final SequenceNumberManager sequenceNumberManager;
    private long messageIdCounter;
    private final Lock sendLock;

    //stats: profiling information
    private final StatsCollector statsCollector;

    public GROUP_MULTICAST() {
        statsCollector = new StatsCollector();
        deliverThread = new DeliverThread(this);
        multicastSenderThread = new SenderThread(this);
        sequenceNumberManager = new SequenceNumberManager();
        sendLock = new ReentrantLock();
        messageIdCounter = 0;
    }

    @Override
    public void start() throws Exception {
        deliverManager = new DeliverManagerImpl();
        senderManager = new SenderManager();
        deliverThread.start(deliverManager);
        multicastSenderThread.clear();
        multicastSenderThread.start();
        statsCollector.setStatsEnabled(statsEnabled());
    }

    @Override
    public void stop() {
        deliverThread.interrupt();
        multicastSenderThread.interrupt();
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                handleDownMessage(evt);
                return null;
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress = (Address) evt.getArg();
                multicastSenderThread.setLocalAddress(localAddress);
                break;
            case Event.VIEW_CHANGE:
                handleViewChange((View) evt.getArg());
                break;
            default:
                break;
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        switch (evt.getType()) {
            case Event.MSG:
                Message message = (Message) evt.getArg();

                GroupMulticastHeader header = (GroupMulticastHeader) message.getHeader(this.id);

                if (header == null) {
                    break;
                }

                switch (header.getType()) {
                    case GroupMulticastHeader.DATA_MESSAGE:
                        handleDataMessage(message, header);
                        break;
                    case GroupMulticastHeader.PROPOSE_MESSAGE:
                        handleSequenceNumberPropose(message.getSrc(), header);
                        break;
                    case GroupMulticastHeader.FINAL_MESSAGE:
                        handleFinalSequenceNumber(header);
                        break;
                    default:
                        throw new IllegalStateException("Unknown header type received " + header);
                }
                return null;
            case Event.VIEW_CHANGE:
                handleViewChange((View) evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress = (Address) evt.getArg();
                multicastSenderThread.setLocalAddress(localAddress);
                break;
            default:
                break;
        }
        return up_prot.up(evt);
    }

    @Override
    public void deliver(Message message) {
        message.setDest(localAddress);

        if (log.isDebugEnabled()) {
            log.debug("Deliver message " + message + " in total order");
        }

        up_prot.up(new Event(Event.MSG, message));
        statsCollector.incrementMessageDeliver();
    }

    private void handleViewChange(View view) {
        if (log.isTraceEnabled()) {
            log.trace("Handle view " + view);
        }
        // TODO: Future work: How to add fault tolerance? (simple and efficient)
    }

    private void handleDownMessage(Event evt) {
        Message message = (Message) evt.getArg();
        Address dest = message.getDest();

        if (dest != null && dest instanceof GroupAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
            //group multicast message
            handleDownGroupMulticastMessage(message);
        } else if (dest != null && dest instanceof GroupAddress) {
            //group address with NO_TOTAL_ORDER flag (should no be possible, but...)
            handleDownGroupMessage(message);
        } else {
            //normal message
            down_prot.down(evt);
        }
    }

    private void handleDownGroupMessage(Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Handle message with Group Address but with Flag NO_TOTAL_ORDER set");
        }
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        try {
            multicastSenderThread.addMessage(message, groupAddress.getAddresses());
        } catch (Exception e) {
            logException("Exception caugh while send a NO-TOTAL-ORDER group multicast", e);
        }
    }

    private void handleDownGroupMulticastMessage(Message message) {
        boolean trace = log.isTraceEnabled();
        boolean warn = log.isWarnEnabled();

        long startTime = statsCollector.now();
        long duration = -1;

        if (trace) {
            log.trace("Handle group multicast message");
        }
        GroupAddress groupAddress = (GroupAddress) message.getDest();
        Set<Address> destination = groupAddress.getAddresses();

        if (destination.isEmpty()) {
            if (warn) {
                log.warn("Received a group address with an empty list");
            }
            throw new IllegalStateException("Group Address must have at least one element");
        }

        if (destination.size() == 1) {
            if (warn) {
                log.warn("Received a group address with an element");
            }
            message.setDest(destination.iterator().next());
            down_prot.down(new Event(Event.MSG, message));
            return;
        }

        boolean deliverToMySelf = destination.contains(localAddress);

        try {
            sendLock.lock();
            MessageID messageID = new MessageID(localAddress, messageIdCounter++);
            long sequenceNumber = sequenceNumberManager.getAndIncrement();

            GroupMulticastHeader header = GroupMulticastHeader.createNewHeader(GroupMulticastHeader.DATA_MESSAGE,
                    messageID);
            header.setSequencerNumber(sequenceNumber);
            header.addDestinations(destination);
            message.putHeader(this.id, header);

            senderManager.addNewMessageToSent(messageID, destination, sequenceNumber, deliverToMySelf);

            if (deliverToMySelf) {
                deliverManager.addNewMessageToDeliver(messageID, message, sequenceNumber);
            }

            if (trace) {
                log.trace("Sending message " + messageID + " to " + destination + " with initial sequence number of " +
                        sequenceNumber);
            }

            multicastSenderThread.addMessage(message, destination);

            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while handling group multicast message. Error is " + e.getLocalizedMessage(),
                    e);
        } finally {
            sendLock.unlock();
            statsCollector.addGroupMulticastSentDuration(duration, (destination.size() - (deliverToMySelf ? 1 : 0)));
        }
    }

    private void handleDataMessage(Message message, GroupMulticastHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;

        try {
            MessageID messageID = header.getMessageID();

            //create the sequence number and put it in deliver manager
            long myProposeSequenceNumber = sequenceNumberManager.updateAndGet(header.getSequencerNumber());
            deliverManager.addNewMessageToDeliver(messageID, message, myProposeSequenceNumber);

            if (log.isTraceEnabled()) {
                log.trace("Received the message with " + header + ". The proposed sequence number is " +
                        myProposeSequenceNumber);
            }

            //create a new message and send it back
            Message proposeMessage = new Message();
            proposeMessage.setSrc(localAddress);
            proposeMessage.setDest(messageID.getAddress());

            GroupMulticastHeader newHeader = GroupMulticastHeader.createNewHeader(
                    GroupMulticastHeader.PROPOSE_MESSAGE, messageID);

            newHeader.setSequencerNumber(myProposeSequenceNumber);
            proposeMessage.putHeader(this.id, newHeader);
            proposeMessage.setFlag(Message.Flag.OOB);
            proposeMessage.setFlag(Message.Flag.DONT_BUNDLE);

            //multicastSenderThread.addUnicastMessage(proposeMessage);
            down_prot.down(new Event(Event.MSG, proposeMessage));
            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the data message " + header.getMessageID(), e);
        } finally {
            statsCollector.addDataMessageDuration(duration);
        }
    }

    private void handleSequenceNumberPropose(Address from, GroupMulticastHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;
        boolean lastProposeReceived = false;

        boolean trace = log.isTraceEnabled();
        try {
            MessageID messageID = header.getMessageID();
            if (trace) {
                log.trace("Received the proposed sequence number message with " + header + " from " +
                        from);
            }

            sequenceNumberManager.update(header.getSequencerNumber());
            long finalSequenceNumber = senderManager.addPropose(messageID, from,
                    header.getSequencerNumber());

            if (finalSequenceNumber != SenderManager.NOT_READY) {
                lastProposeReceived = true;
                Message finalMessage = new Message();
                finalMessage.setSrc(localAddress);

                GroupMulticastHeader finalHeader = GroupMulticastHeader.createNewHeader(
                        GroupMulticastHeader.FINAL_MESSAGE, messageID);

                finalHeader.setSequencerNumber(finalSequenceNumber);
                finalMessage.putHeader(this.id, finalHeader);
                finalMessage.setFlag(Message.Flag.OOB);
                finalMessage.setFlag(Message.Flag.DONT_BUNDLE);

                Set<Address> destination = senderManager.getDestination(messageID);
                if (destination.contains(localAddress)) {
                    destination.remove(localAddress);
                }

                if (trace) {
                    log.trace("Message " + messageID + " is ready to be deliver. Final sequencer number is " +
                            finalSequenceNumber);
                }

                multicastSenderThread.addMessage(finalMessage, destination);
                //returns true if we are in destination set
                if (senderManager.markSent(messageID)) {
                    deliverManager.markReadyToDeliver(messageID, finalSequenceNumber);
                }
            }

            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the propose sequence number for " + header.getMessageID(), e);
        } finally {
            statsCollector.addProposeSequenceNumberDuration(duration, lastProposeReceived);
        }
    }

    private void handleFinalSequenceNumber(GroupMulticastHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;

        try {
            MessageID messageID = header.getMessageID();
            if (log.isTraceEnabled()) {
                log.trace("Received the final sequence number message with " + header);
            }

            sequenceNumberManager.update(header.getSequencerNumber());
            deliverManager.markReadyToDeliver(messageID, header.getSequencerNumber());
            duration = statsCollector.now() - startTime;
        } catch (Exception e) {
            logException("Exception caught while processing the final sequence number for " + header.getMessageID(), e);
        } finally {
            statsCollector.addFinalSequenceNumberDuration(duration);
        }
    }

    private void logException(String msg, Exception e) {
        if (log.isDebugEnabled()) {
            log.debug(msg, e);
        } else if (log.isWarnEnabled()) {
            log.warn(msg + ". Error is " + e.getLocalizedMessage());
        }
    }

    @ManagedOperation
    public String getMessageList() {
        return deliverManager.getMessageSet().toString();
    }

    @Override
    public void enableStats(boolean flag) {
        super.enableStats(flag);
        statsCollector.setStatsEnabled(flag);
    }

    @Override
    public void resetStats() {
        super.resetStats();
        statsCollector.clearStats();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing and sending the group " +
            "multicast message to all the recipients", writable = false)
    public double getAvgGroupMulticastSentDuration() {
        return statsCollector.getAvgGroupMulticastSentDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a data message received",
            writable = false)
    public double getAvgDataMessageReceivedDuration() {
        return statsCollector.getAvgDataMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a propose message received" +
            "(not the last one", writable = false)
    public double getAvgProposeMessageReceivedDuration() {
        return statsCollector.getAvgProposeMesageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing the last propose message " +
            "received. This last propose message will originate the sending of the final message", writable = false)
    public double getAvgLastProposeMessageReceivedDuration() {
        return statsCollector.getAvgLastProposeMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a final message received",
            writable = false)
    public double getAvgFinalMessageReceivedDuration() {
        return statsCollector.getAvgFinalMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The number of group multicast messages sent", writable = false)
    public int getNumberOfGroupMulticastMessagesSent() {
        return statsCollector.getNumberOfGroupMulticastMessagesSent();
    }

    @ManagedAttribute(description = "The number of final group messages sent", writable = false)
    public int getNumberOfFinalGroupMessagesSent() {
        return statsCollector.getNumberOfFinalGroupMessagesSent();
    }

    @ManagedAttribute(description = "The number of group multicast messages delivered", writable = false)
    public int getNumberOfGroupMulticastMessagesDelivered() {
        return statsCollector.getGroupMulticastDelivered();
    }

    @ManagedAttribute(description = "The number of propose messages sent", writable = false)
    public int getNumberOfProposeMessageSent() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of final messages delivered", writable = false)
    public int getNumberOfFinalMessagesDelivered() {
        return statsCollector.getNumberOfFinalMessagesDelivered();
    }

    @ManagedAttribute(description = "The number of data messages delivered", writable = false)
    public int getNumberOfDataMessagesDelivered() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of propose messages received", writable = false)
    public int getNumberOfProposeMessageReceived() {
        return statsCollector.getNumberOfProposeMessagesReceived();
    }

    @ManagedAttribute(description = "The average number of unicasts messages created per group multicast message",
            writable = false)
    public double getAvgNumberOfUnicastSentPerGroupMulticast() {
        return statsCollector.getAvgNumberOfUnicastSentPerGroupMulticast();
    }
}
