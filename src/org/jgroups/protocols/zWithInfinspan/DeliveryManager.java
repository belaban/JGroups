package org.jgroups.protocols.zWithInfinspan;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class, and associated inner classes, that are responsible for ensuring that abcast messages are delivered by client nodes
 * in the correct total order.
 *
 * @author Ryan Emerson
 * @since 4.0
 */
public class DeliveryManager {
	
    private final SortedSet<MessageRecord> deliverySet;
    private final ViewManager viewManager;
    private final AtomicLong lastDelivered;
    private final Log log;
    private Address localAddress;

    public DeliveryManager(Log log, ViewManager viewManager) {
        this.log = log;
        this.viewManager = viewManager;
        deliverySet = Collections.synchronizedSortedSet(new TreeSet<MessageRecord>());
        lastDelivered = new AtomicLong();
    }
    
    public void setLocalAddress(Address localAddress) {
        this.localAddress = localAddress;
    }

    public void addSingleDestinationMessage(Message msg) {
        synchronized (deliverySet) {
            deliverySet.notify();
        }
    }

    public List<Message> getNextMessagesToDeliver() throws InterruptedException {
    	log.info("getNextMessagesToDeliver >> ");

        LinkedList<Message> msgsToDeliver = new LinkedList<Message>();
        synchronized (deliverySet) {
            while (deliverySet.isEmpty()) {
                deliverySet.wait();
            }

            if (!deliverySet.first().isDeliverable) {
                deliverySet.wait();
            }

            Iterator<MessageRecord> iterator = deliverySet.iterator();
            while (iterator.hasNext()) {
                MessageRecord record = iterator.next();
                if (record.isDeliverable) {
                	log.info("Deliver message in order of >> " + record.getHeader().getMessageOrderInfo().getOrdering());
                    msgsToDeliver.add(record.message);
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        return msgsToDeliver;
    }

    public void addMessageToDeliver(CSInteractionHeader header, Message message, boolean local) {
        if (header.getMessageOrderInfo().getOrdering() <= lastDelivered.longValue()) {
            if (log.isDebugEnabled())
                log.debug("Message already received or Missed! | " + header.getMessageOrderInfo().getOrdering() + " | local := " + local);
            return;
        }

        if (log.isTraceEnabled())
            log.trace("Add message to deliver | " + header.getMessageOrderInfo() + " | lastDelivered := " + lastDelivered.longValue());

        MessageRecord record = new MessageRecord(header, message);
        synchronized (deliverySet) {
            readyToDeliver(record);
            deliverySet.add(record);
            MessageRecord firstRecord = deliverySet.first();
            if (firstRecord.isDeliverable) {
                recheckRecords(firstRecord);
                deliverySet.notify();
            }
        }
    }

    private boolean readyToDeliver(MessageRecord record) {
    	CSInteractionHeader header = record.header;
        MessageOrderInfo messageOrderInfo = header.getMessageOrderInfo();
        long thisMessagesOrder = messageOrderInfo.getOrdering();
        long lastDelivery = lastDelivered.longValue();

        long lastOrdering = viewManager.getClientLastOrdering(messageOrderInfo, localAddress);
        //log.info(" lastOrdering =" + lastOrdering + " / " + "lastDelivery =" + lastDelivery
        		//+ " / " + "thisMessagesOrder =" + thisMessagesOrder);
        if (lastOrdering < 0 || (lastOrdering > 0 && lastOrdering <= lastDelivery)) {
            // lastOrder has already been delivered, so this message is deliverable
        	// log.info(" condition true, so deliver thisMessagesOrder " + thisMessagesOrder);
            record.isDeliverable = true;
            lastDelivered.set(thisMessagesOrder);
            return true;
        }
        if (log.isTraceEnabled())
            log.trace("Previous message not received | " + thisMessagesOrder + " | require " + lastOrdering);
        return false;
    }

    private void recheckRecords(MessageRecord record) {
        Set<MessageRecord> followers = deliverySet.tailSet(record);
        for (MessageRecord r : followers) {
            if (r.equals(record))
                continue;
            if (!readyToDeliver(r))
                return;
        }
    }

    class MessageRecord implements Comparable<MessageRecord> {
        final private CSInteractionHeader header;
        final private Message message;
        private volatile boolean isDeliverable;

        private MessageRecord(CSInteractionHeader header, Message message) {
            this.header = header;
            this.message = message;
            this.isDeliverable = false;
            this.message.setSrc(header.getMessageOrderInfo().getId().getOriginator());
        }
        
       
        public CSInteractionHeader getHeader() {
			return header;
		}

		public Message getMessage() {
			return message;
		}

		@Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageRecord that = (MessageRecord) o;

            if (header != null ? !header.equals(that.header) : that.header != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return header != null ? header.hashCode() : 0;
        }

        @Override
        public int compareTo(MessageRecord nextRecord) {
            long thisOrdering = header.getMessageOrderInfo().getOrdering();
            long nextOrdering = nextRecord.header.getMessageOrderInfo().getOrdering();

            if (this.equals(nextRecord))
                return 0;
            else if (thisOrdering > nextOrdering)
                return 1;
            else
                return -1;
        }
        

        @Override
        public String toString() {
            return "MessageRecord{" +
                    "header=" + header +
                    ", isDeliverable=" + isDeliverable +
                    '}';
        }
    }
}