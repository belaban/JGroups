package org.jgroups.protocols.jzookeeper;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.jzookeeper.Zab.FollowerMessageHandler;
import org.jgroups.protocols.jzookeeper.Zab.MessageHandler;
import org.jgroups.protocols.jzookeeper.Zab.Throughput;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

public class ZabClientHandler extends Protocol {
	
	private final static String ProtocolName = "Zab";
	private Address local_addr;
	private volatile Address leader;
	private volatile View view;
	private volatile boolean is_leader = false;
	private List<Address> zabMembers = Collections
			.synchronizedList(new ArrayList<Address>());
	private final LinkedBlockingQueue<Message> requestHandler = new LinkedBlockingQueue<Message>();
	private final Map<MessageId, Message> messageStore = Collections
			.synchronizedMap(new HashMap<MessageId, Message>());
	private int index = -1;
	private volatile boolean running = true;
	private final static String outDir = "/work/Zab/";
	private ExecutorService executor;
	private Calendar cal = Calendar.getInstance();

	/*
	 * Empty constructor
	 */
	public ZabClientHandler() {

	}

	@ManagedAttribute
	public boolean isleaderinator() {
		return is_leader;
	}

	public Address getleaderinator() {
		return leader;
	}

	public Address getLocalAddress() {
		return local_addr;
	}

	@ManagedOperation
	public String printStats() {
		return dumpStats().toString();
	}

	@Override
	public void start() throws Exception {
		super.start();
		running = true;
		executor.execute(new RequestHandler());
		log.setLevel("trace");

	}

	@Override
	public void stop() {
		running = false;
		super.stop();
	}

	public Object down(Event evt) {
		switch (evt.getType()) {
		case Event.MSG:
			Message m = (Message) evt.getArg();
			requestHandler.add(m);
			return null; // don't pass down
		case Event.SET_LOCAL_ADDRESS:
			local_addr = (Address) evt.getArg();
			break;
		}
		return down_prot.down(evt);
	}

	public Object up(Event evt) {
		Message msg = null;
		ZabHeader hdr;

		switch (evt.getType()) {
		case Event.MSG:
			msg = (Message) evt.getArg();
			hdr = (ZabHeader) msg.getHeader(this.id);
			if (hdr == null) {
				break; // pass up
			}
			switch (hdr.getType()) {
			case ZabHeader.START_SENDING:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
	
			case ZabHeader.STARTREALTEST:
				if (!zabMembers.contains(local_addr))
					return up_prot.up(new Event(Event.MSG, msg));
				break;
			case ZabHeader.RESPONSE:
				handleOrderingResponse(hdr);
			}
			return null;
		case Event.VIEW_CHANGE:
			handleViewChange((View) evt.getArg());
			break;

		}

		return up_prot.up(evt);
	}

	public void up(MessageBatch batch) {
		for (Message msg : batch) {
			if (msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER)
					|| msg.isFlagSet(Message.Flag.OOB)
					|| msg.getHeader(id) == null)
				continue;
			batch.remove(msg);

			try {
				up(new Event(Event.MSG, msg));
			} catch (Throwable t) {
				log.error("failed passing up message", t);
			}
		}

		if (!batch.isEmpty())
			up_prot.up(batch);
	}

	/*
	 * --------------------------------- Private Methods
	 * --------------------------------
	 */

	/*
	 * Handling all client requests, processing them according to request type
	 */
	private void handleClientRequest(Message message) {
		ZabHeader clientHeader = ((ZabHeader) message.getHeader(this.id));

		if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.START_SENDING) {
			for (Address client : view.getMembers()) {
				if (log.isDebugEnabled())
				if (!zabMembers.contains(client)) {
					if (log.isDebugEnabled())
						log.info("Address to check is not zab Members, will send start request to"
								+ client + " " + getCurrentTimeStamp());
					message.setDest(client);
					down_prot.down(new Event(Event.MSG, message));
				}
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.RESET) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				resetMessage.setSrc(local_addr);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.STATS) {

			for (Address server : zabMembers) {
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}
		
		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.CLIENTFINISHED) {
			for (Address server : zabMembers) {
					Message countMessages = new Message(server).putHeader(
							this.id, clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
			}

		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZabHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
					clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
			// int diff = 1000 - hdrReq.size();
			// if (diff > 0)
			// requestMessage.setBuffer(new byte[diff]);
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZabHeader.SENDMYADDRESS) {
			Address destination = null;
			destination = zabMembers.get(0);
			// destination = zabMembers.get(0);
			log.info("ZabMemberSize = " + zabMembers.size());
			for (Address server : zabMembers) {
				log.info("server address = " + server);
				message.dest(server);
				message.src(message.getSrc());
				down_prot.down(new Event(Event.MSG, message));
			}
		}

	}

	private void handleViewChange(View v) {
		List<Address> mbrs = v.getMembers();
		leader = mbrs.get(0);
		if (leader.equals(local_addr)) {
			is_leader = true;
		}
		// make the first three joined server as ZK servers
		if (mbrs.size() == 3) {
			zabMembers.addAll(v.getMembers());
			if (zabMembers.contains(local_addr)) {
				log.info("(going to executor2, handleViewChange  ");
			}

		}
		if (mbrs.size() > 3 && zabMembers.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabMembers.add(mbrs.get(i));
			}

		}
		if (mbrs.isEmpty())
			return;

		if (view == null || view.compareTo(v) < 0)
			view = v;
		else
			return;
	}

	/*
	 * Send replay to client
	 */
	private void handleOrderingResponse(ZabHeader hdrResponse) {
		Message message = messageStore.get(hdrResponse.getMessageId());
		message.putHeader(this.id, hdrResponse);
		up_prot.up(new Event(Event.MSG, message));
		messageStore.remove(hdrResponse.getMessageId());

	}

	
	private String getCurrentTimeStamp() {
		long timestamp = new Date().getTime();
		cal.setTimeInMillis(timestamp);
		String timeString = new SimpleDateFormat("HH:mm:ss:SSS").format(cal
				.getTime());

		return timeString;
	}
	/*
	 * ----------------------------- End of Private Methods
	 * --------------------------------
	 */

	
	final class RequestHandler implements Runnable {
        @Override
        public void run() {
           	deliverMessages();

        }

        private void deliverMessages() {
			Message request= null;
            while (true) {
    				try {
    					request = requestHandler.take();
    					//log.info("(deliverMessages) deliver zxid = "+ hdrDelivery.getZxid());
    				} catch (InterruptedException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}          
					//log.info("(going to call deliver zxid =  "+ hdrDelivery.getZxid());
    				handleClientRequest(request);
                        
            }
        }
  
    } 
	
	
}