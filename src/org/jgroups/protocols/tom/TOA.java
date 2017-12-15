package org.jgroups.protocols.tom;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;

/**
 * Total Order Anycast with three communication steps (based on Skeen's Algorithm). Establishes total order for a
 * message sent to a subset of the cluster members (an anycast). Example: send a totally ordered message to {D,E}
 * out of a membership of {A,B,C,D,E,F}.<p/>
 * Skeen's algorithm uses consensus among the anycast target members to find the currently highest
 * sequence number (seqno) and delivers the message according to the order established by the seqnos.
 *
 * @author Pedro Ruivo
 * @since 3.1
 */
@MBean(description = "Implementation of Total Order Anycast based on Skeen's Algorithm")
public class TOA extends Protocol implements DeliveryProtocol {
    //managers
    private DeliveryManagerImpl deliverManager;
    private SenderManager senderManager;

    // threads
    private volatile DeliveryThread deliverThread = new DeliveryThread(this);

    //local address
    private Address localAddress;

    //sequence numbers, messages ids and lock
    private final AtomicLong messageIdCounter = new AtomicLong(0);

    //stats: profiling information
    private final StatsCollector statsCollector = new StatsCollector();

    public TOA() {
    }

    @Override
    public void start() throws Exception {
        deliverManager = new DeliveryManagerImpl();
        senderManager = new SenderManager();
        if(deliverThread == null) {
            deliverThread=new DeliveryThread(this);
            deliverThread.setLocalAddress(localAddress.toString());
        }
        deliverThread.start(deliverManager);
        statsCollector.setStatsEnabled(statsEnabled());
    }

    @Override
    public void stop() {
        deliverThread.interrupt();
        deliverThread=null;
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress =evt.getArg();
                if(this.deliverThread != null)
                    this.deliverThread.setLocalAddress(localAddress.toString());
                break;
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;
            default:
                break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message message) {
        Address dest = message.getDest();

        if (dest != null && dest instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
            // anycast message
            sendTotalOrderAnycastMessage(extract((AnycastAddress) dest), message);
        } else if (dest != null && dest instanceof AnycastAddress) {
            //anycast address with NO_TOTAL_ORDER flag (should no be possible, but...)
            send(extract((AnycastAddress) dest), message, true);
        } else {
            //normal message
            down_prot.down(message);
        }
        return null;
    }

    @Override
    public Object up(Event evt) {
        switch (evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(evt.getArg());
                break;
            case Event.SET_LOCAL_ADDRESS:
                this.localAddress =evt.getArg();
                this.deliverThread.setLocalAddress(localAddress.toString());
                break;
            default:
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message message) {
        ToaHeader header=message.getHeader(this.id);

        if (header == null)
            return up_prot.up(message);

        switch (header.getType()) {
            case ToaHeader.DATA_MESSAGE:
                handleDataMessage(message, header);
                break;
            case ToaHeader.PROPOSE_MESSAGE:
                handleSequenceNumberPropose(message.getSrc(), header);
                break;
            case ToaHeader.FINAL_MESSAGE:
                handleFinalSequenceNumber(header);
                break;
            case ToaHeader.SINGLE_DESTINATION_MESSAGE:
                if (log.isTraceEnabled()) {
                    log.trace("Received message %s with SINGLE_DESTINATION header. delivering...", message);
                }
                deliverManager.deliverSingleDestinationMessage(message, header.getMessageID());
                break;
            default:
                throw new IllegalStateException("Unknown header type received " + header);
        }
        return null;
    }

    @Override
    public void deliver(Message message) {
        message.setDest(localAddress);

        if (log.isTraceEnabled()) {
            log.trace("Deliver message %s (%s) in total order", message, message.getHeader(id));
        }

        up_prot.up(message);
        statsCollector.incrementMessageDeliver();
    }

    private void handleViewChange(View view) {
        if (log.isTraceEnabled()) {
            log.trace("Handle view %s", view);
        }
        final Collection<Address> leavers = deliverManager.handleView(view);
        //basis behavior: drop leavers message (as senders)

        //basis behavior: avoid waiting for the acks
        Collection<MessageID> pendingSentMessages = senderManager.getPendingMessageIDs();
        for (MessageID messageID : pendingSentMessages) {
            long finalSequenceNumber = senderManager.removeLeavers(messageID, leavers);
            if (finalSequenceNumber != SenderManager.NOT_READY) {
                ToaHeader finalHeader = ToaHeader.newFinalMessageHeader(messageID, finalSequenceNumber);
                Message finalMessage = new Message().src(localAddress).putHeader(this.id, finalHeader)
                        .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

                if (log.isTraceEnabled()) {
                    log.trace("Message %s is ready to be delivered. Final sequencer number is %d",
                              messageID, finalSequenceNumber);
                }

                send(senderManager.getDestination(messageID), finalMessage, false);
                //returns true if we are in destination set
                if (senderManager.markSent(messageID)) {
                    deliverManager.markReadyToDeliver(messageID, finalSequenceNumber);
                }
            }
        }
        // TODO: Future work: How to add fault tolerance? (simple and efficient)
    }


    private void sendTotalOrderAnycastMessage(Collection<Address> destinations, Message message) {
        boolean trace = log.isTraceEnabled();

        long startTime = statsCollector.now();

        final boolean deliverToMySelf = destinations.contains(localAddress);
        final MessageID messageID = generateId();

        if (destinations.size() == 1) {
            message.putHeader(id, ToaHeader.createSingleDestinationHeader(messageID));
            message.setDest(destinations.iterator().next());

            if (trace) {
                log.trace("Sending total order anycast message %s (%s) to single destination", message, message.getHeader(id));
            }

            if (deliverToMySelf) {
                deliverManager.deliverSingleDestinationMessage(message, messageID);
            } else {
                down_prot.down(message);
            }
            return;
        }

        try {
            ToaHeader header = ToaHeader.newDataMessageHeader(messageID, deliverManager.getViewId());
            message.putHeader(this.id, header);

            //sets the sequence number in header!
            long sequenceNumber = deliverToMySelf ?
                                  deliverManager.addLocalMessageToDeliver(messageID, message, header) :
                                  -1;

            if (trace) {
                log.trace("Sending total order anycast message %s (%s) to %s", message, message.getHeader(id), destinations);
            }

            senderManager.addNewMessageToSend(messageID, destinations, sequenceNumber, deliverToMySelf);
            send(destinations, message, false);
        } catch (Exception e) {
            logException("Exception caught while sending anycast message. Error is " + e.getLocalizedMessage(),
                    e);
        } finally {
            long duration = statsCollector.now() - startTime;
            statsCollector.addAnycastSentDuration(duration, (destinations.size() - (deliverToMySelf ? 1 : 0)));
        }
    }

    private MessageID generateId() {
        return new MessageID(localAddress, messageIdCounter.getAndIncrement());
    }

    private void send(Collection<Address> destinations, Message msg, boolean sendToMyself) {
        if (log.isTraceEnabled()) {
            log.trace("sending anycast total order message %s to %s", msg, destinations);
        }
        for (Address address : destinations) {
            if (!sendToMyself && address.equals(localAddress)) {
                continue;
            }
            Message cpy = msg.copy();
            cpy.setDest(address);
            down_prot.down(cpy);
        }
    }

    private void handleDataMessage(Message message, ToaHeader header) {
        final long startTime = statsCollector.now();

        try {
            final MessageID messageID = header.getMessageID();

            //create the sequence number and put it in deliver manager
            long myProposeSequenceNumber = deliverManager.addRemoteMessageToDeliver(messageID, message,
                    header.getSequencerNumber(), header.getViewId());

            if (log.isTraceEnabled()) {
                log.trace("Received the message with %s. The proposed sequence number is %d",
                          header, myProposeSequenceNumber);
            }

            if (myProposeSequenceNumber == -1) {
                //message discarded. not sending ack back.
                return;
            }

            //create a new message and send it back
            ToaHeader newHeader = ToaHeader.newProposeMessageHeader(messageID, myProposeSequenceNumber);

            Message proposeMessage = new Message().src(localAddress).dest(messageID.getAddress())
                    .putHeader(this.id, newHeader).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

            //multicastSenderThread.addUnicastMessage(proposeMessage);
            down_prot.down(proposeMessage);
        } catch (Exception e) {
            logException("Exception caught while processing the data message " + header.getMessageID(), e);
        } finally {
            statsCollector.addDataMessageDuration(statsCollector.now() - startTime);
        }
    }

    private void handleSequenceNumberPropose(Address from, ToaHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;
        boolean lastProposeReceived = false;

        boolean trace = log.isTraceEnabled();
        try {
            MessageID messageID = header.getMessageID();
            if (trace) {
                log.trace("Received the proposed sequence number message with %s from %s",
                        header, from);
            }

            deliverManager.updateSequenceNumber(header.getSequencerNumber());
            long finalSequenceNumber = senderManager.addPropose(messageID, from,
                    header.getSequencerNumber());

            if (finalSequenceNumber != SenderManager.NOT_READY) {
                lastProposeReceived = true;

                ToaHeader finalHeader = ToaHeader.newFinalMessageHeader(messageID, finalSequenceNumber);

                Message finalMessage = new Message().src(localAddress).putHeader(this.id, finalHeader)
                        .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE);

                Set<Address> destinations = senderManager.getDestination(messageID);
                if (destinations.contains(localAddress)) {
                    destinations.remove(localAddress);
                }

                if (trace) {
                    log.trace("Message %s is ready to be delivered. Final sequencer number is %d",
                                messageID, finalSequenceNumber);
                }

                send(destinations, finalMessage, false);
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

    private void handleFinalSequenceNumber(ToaHeader header) {
        long startTime = statsCollector.now();
        long duration = -1;

        try {
            MessageID messageID = header.getMessageID();
            if (log.isTraceEnabled()) {
                log.trace("Received the final sequence number message with %s", header);
            }

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
            log.warn("%s. Error is %s", msg, e.getLocalizedMessage());
        }
    }

    private Collection<Address> extract(AnycastAddress anycastAddress) {
        return anycastAddress.findAddresses().orElseGet(deliverManager::getViewMembers);
    }

    @ManagedOperation
    public String getMessageList() {
        return deliverManager.getMessageSet().toString();
    }

    public DeliveryManager getDeliverManager() {
        return deliverManager;
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

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing and sending the anycast " +
            "message to all the recipients")
    public double getAvgToaSendDuration() {
        return statsCollector.getAvgAnycastSentDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a data message received")
    public double getAvgDataMessageReceivedDuration() {
        return statsCollector.getAvgDataMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a propose message received" +
            "(not the last one")
    public double getAvgProposeMessageReceivedDuration() {
        return statsCollector.getAvgProposeMesageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing the last propose message " +
            "received. This last propose message will originate the sending of the final message")
    public double getAvgLastProposeMessageReceivedDuration() {
        return statsCollector.getAvgLastProposeMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The average duration (in milliseconds) in processing a final message received")
    public double getAvgFinalMessageReceivedDuration() {
        return statsCollector.getAvgFinalMessageReceivedDuration();
    }

    @ManagedAttribute(description = "The number of anycast messages sent")
    public int getNumberOfAnycastMessagesSent() {
        return statsCollector.getNumberOfAnycastMessagesSent();
    }

    @ManagedAttribute(description = "The number of final anycast sent")
    public int getNumberOfFinalAnycastSent() {
        return statsCollector.getNumberOfFinalAnycastsSent();
    }

    @ManagedAttribute(description = "The number of anycast messages delivered")
    public int getNumberOfAnycastMessagesDelivered() {
        return statsCollector.getAnycastDelivered();
    }

    @ManagedAttribute(description = "The number of propose messages sent")
    public int getNumberOfProposeMessageSent() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of final messages delivered")
    public int getNumberOfFinalMessagesDelivered() {
        return statsCollector.getNumberOfFinalMessagesDelivered();
    }

    @ManagedAttribute(description = "The number of data messages delivered")
    public int getNumberOfDataMessagesDelivered() {
        return statsCollector.getNumberOfProposeMessagesSent();
    }

    @ManagedAttribute(description = "The number of propose messages received")
    public int getNumberOfProposeMessageReceived() {
        return statsCollector.getNumberOfProposeMessagesReceived();
    }

    @ManagedAttribute(description = "The average number of unicasts messages created per anycast message")
    public double getAvgNumberOfUnicastSentPerAnycast() {
        return statsCollector.getAvgNumberOfUnicastSentPerAnycast();
    }
}
