package org.jgroups.protocols.jzookeeper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

public class CSInteraction extends Protocol {
	

	public static int minimumServers = 3; // Static hack to allow experiments to dynamically change the value.

    @Property(name = "service_member", description = "Is this process a service member")
    private boolean zMember = false;

    @Property(name = "z_members", description = "A list of hostnames that will be Z members (seperated by a colon)")
    private String boxHostnames = "";

    @Property(name = "msg_size", description = "The max size of a msg between Z members.  Determines the number of msgs that can be bundled")
    private int MSG_SIZE = 1000;

    @Property(name = "queue_capacity", description = "The maximum number of ordering requests that can be queued")
    private int QUEUE_CAPACITY = 500;

    @Property(name = "bundle_msgs", description = "If true then ordering requests will be bundled when possible" +
            "in order to reduce the number of total order broadcasts between box members")
    
    private View view = null;
    
    private final ViewManager viewManager = new ViewManager();
    
    private final DeliveryManager deliveryManager = new DeliveryManager(log, viewManager);

    private boolean BUNDLE_MSGS = true;

    private Address leader = null;

    private Address local_addr = null;
    
    private int index = 0;
    
    private AtomicLong local = new AtomicLong(0);
    
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private final List<Address> zabMembers = new ArrayList<Address>();
    private AtomicInteger localSequence = new AtomicInteger(); // This nodes sequence number
    private ExecutorService executor;

    public CSInteraction() {
    }

    private void logHack() {
        Logger logger = Logger.getLogger(this.getClass().getName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    @Override
    public void init() throws Exception {
        logHack();
        setLevel("info");

     }

    
    @Override
    public void start() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new MessageHandler());
    }

    @Override
    public void stop() {
        executor.shutdown();
    }

    @Override
    public Object up(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                Message message = (Message) event.getArg();
                CSInteractionHeader header = (CSInteractionHeader) message.getHeader(id);
            	log.info("CSInteractionHeader.RESPONSE: "+header);

                if (header == null)
                    break;

                switch (header.getType()) {
                    case CSInteractionHeader.RESPONSE:
                        handleOrderingResponse(header);
                        break;                
                }
                return null;
            case Event.VIEW_CHANGE:
                handleViewChange((View) event.getArg());
                if (log.isTraceEnabled())
                    log.trace("New View := " + view);
                break;
        }
        return up_prot.up(event);
    }

    @Override
    public Object down(Event event) {
        switch (event.getType()) {
            case Event.MSG:
                //handleMessageRequest(event);
               // return null;
                
                Message message = (Message) event.getArg();
                CSInteractionHeader header = (CSInteractionHeader) message.getHeader(id);
    			if (header instanceof CSInteractionHeader){
    				handleMessageRequest(message);
    				return null;
    			}
    			else{
    				break;
    			}
            case Event.SET_LOCAL_ADDRESS:
                local_addr = (Address) event.getArg();
                break;
        }
        return down_prot.down(event);
    }

    	/*
    	 * Handling all client requests, processing them according to request type
    	 */
    
    private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		//make the first three joined server as ZK servers
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
		}
		if (mbrs.size() > 3 && zabMembers.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabMembers.add(mbrs.get(i));
			}

		}

		log.info("zabMembers size = " + zabMembers);
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0){
			view = v;
        	viewManager.setCurrentView(view);
		}
		else
			return;
	}

    private void handleMessageRequest(Message message) {
        Address destination = message.getDest();
        //Store put here, and Forward write to Z to obtain ordering
        if (destination != null && destination instanceof AnycastAddress && !message.isFlagSet(Message.Flag.NO_TOTAL_ORDER)) {
    		log.info("AnycastAddress  destination " + message);

        	sendOrderingRequest(((AnycastAddress) destination).getAddresses(), message);
        }
 
       // else if (destination != null && !(destination instanceof AnycastAddress)) {
			//down_prot.down(new Event(Event.MSG, message));
		//}

	}
    
    private synchronized void sendOrderingRequest(Collection<Address> destinations, Message message) {
    	MessageId messageId = new MessageId(local_addr, local.getAndIncrement());
        messageStore.put(messageId, message);
        byte[] dest = viewManager.getDestinationsAsByteArray(destinations);        
        MessageOrderInfo messageOrderInfo = new MessageOrderInfo(messageId, dest);
        ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
        		messageOrderInfo);
        index++;
        if (index > 2)
            index=0;
        Message requestMessage = new Message(zabMembers.get(index)).putHeader(
        (short) 75, hdrReq);
        //int diff = 1000 - hdrReq.size();
        //if (diff > 0)
        //message.setBuffer(new byte[diff]); // Necessary to ensure that each msgs size is 1kb, necessary for accurate network measurements

        down_prot.down(new Event(Event.MSG, requestMessage));
            
    }

    private synchronized void handleOrderingResponse(CSInteractionHeader responseHeader) {

        if (log.isTraceEnabled())
            log.trace("Ordering response received | " + responseHeader);

        MessageOrderInfo MessageOrderInfo = responseHeader.getMessageOrderInfo();
        CSInteractionHeader header = new CSInteractionHeader(CSInteractionHeader.BROADCAST, MessageOrderInfo);
        Message message = messageStore.get(MessageOrderInfo.getId());
        message.putHeader(id, header);
        broadcastMessage(((AnycastAddress) message.getDest()).getAddresses() , message);

    }

      
    private synchronized void broadcastMessage(Collection<Address> destinations, Message message) {

        if (log.isTraceEnabled())
            log.trace("Broadcast Message to | " + destinations);

        boolean deliverToSelf = destinations.contains(local_addr);
        // Send the message to all destinations
        if (log.isTraceEnabled())
            log.trace("Broadcast Message " + ((CSInteractionHeader) message.getHeader(id)).getMessageOrderInfo().getOrdering() +
                    " to | " + destinations + " | deliverToSelf " + deliverToSelf);

        for (Address destination : destinations) {
            if (destination.equals(local_addr))
                continue;

            Message messageCopy = message.copy();
            messageCopy.setDest(destination);
            down_prot.down(new Event(Event.MSG, messageCopy));
        }


        CSInteractionHeader header = (CSInteractionHeader)message.getHeader(id);
        if (deliverToSelf) {
            message.setDest(local_addr);
            deliveryManager.addMessageToDeliver(header, message, true);
        } else {
            messageStore.remove(header.getMessageOrderInfo().getId());
        }

    }

    private synchronized void handleBroadcast(CSInteractionHeader header, Message message) {

        if (log.isTraceEnabled())
            log.trace("Broadcast received | " + header.getMessageOrderInfo().getOrdering() + " | Src := " + message.getSrc());
        deliveryManager.addMessageToDeliver(header, message, false);
    }

    private synchronized void deliverMessage(Message message) {
        MessageId id = ((CSInteractionHeader)message.getHeader(this.id)).getMessageOrderInfo().getId();
        messageStore.remove(id);
        message.setDest(local_addr);

        if (log.isTraceEnabled())
            log.trace("Deliver Message | " + (CSInteractionHeader) message.getHeader(this.id));

        up_prot.up(new Event(Event.MSG, message));
    }

    final class MessageHandler implements Runnable {
        @Override
        public void run() {
            if (!zabMembers.contains(local_addr))
                deliverMessages();
        }

        private void deliverMessages() {
            while (true) {
                try {
                    List<Message> messages = deliveryManager.getNextMessagesToDeliver();
                    for (Message message : messages) {
                        try {
                            deliverMessage(message);
                        } catch (Throwable t) {
                            if (log.isWarnEnabled())
                                log.warn("AbaaS: Exception caught while delivering message " + message + ":" + t.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    
}
