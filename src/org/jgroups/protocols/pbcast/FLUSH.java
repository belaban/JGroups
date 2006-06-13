//$Id$
package org.jgroups.protocols.pbcast;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.stack.Protocol;

/**
 * Flush, as it name implies, forces group members to flush their pending messages 
 * while blocking them to send any additional messages. The process of flushing 
 * acquiesces the group so that state transfer or a join can be done. It is also 
 * called stop-the-world model as nobody will be able to send messages while a 
 * flush is in process.
 * 
 * <p>
 * Flush is needed for:
 * <p>
 * (1) 	State transfer. When a member requests state transfer, the coordinator 
 * 		tells everyone to stop sending messages and waits for everyone's ack. Then it asks
 *  	the application for its state and ships it back to the requester. After the 
 *  	requester has received and set the state successfully, the coordinator tells 
 *  	everyone to resume sending messages.
 * <p>  
 * (2) 	View changes (e.g.a join). Before installing a new view V2, flushing would 
 * 		ensure that all messages *sent* in the current view V1 are indeed *delivered* 
 * 		in V1, rather than in V2 (in all non-faulty members). This is essentially 
 * 		Virtual Synchrony.
 * 
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 2.4
 */
public class FLUSH extends Protocol {

	public static final String NAME = "FLUSH";
	private View currentView;
	private Address localAddress;
	private Address flushCaller;
	private Collection flushMembers;
	private Set flushOkSet;
	private Set flushCompletedSet;
	private final Object sharedLock = new Object();
	private final Object blockMutex=new Object();
	private volatile boolean isBlockState = false;
	private long timeout = 4000;
	
	public FLUSH() {
		super();
		//tmp view to avoid NPE
		currentView = new View(null,new Vector());
		flushOkSet = new TreeSet();
		flushCompletedSet = new TreeSet();
		flushMembers=new ArrayList();
	}

	public String getName() {
		return NAME;
	}

	public boolean setProperties(Properties props) {
		String str;

		super.setProperties(props);
		str = props.getProperty("timeout");
		if (str != null) {
			timeout  = Long.parseLong(str);
			props.remove("timeout");
		}

		if (props.size() > 0) {
			log.error("FLUSH.setProperties(): the following properties are not recognized: "
							+ props);

			return false;
		}
		return true;
	}	
	
	public void down(Event evt) {
		switch (evt.getType()) {
			case Event.MSG:
				synchronized (blockMutex) {
					while (isFlushRunning()) {
						log.debug("FLUSH block at  " + localAddress + " for " + timeout);
						try {							
							blockMutex.wait(timeout);
							if (isFlushRunning()) {
								log.warn("Forcing FLUSH unblock at " + localAddress);
								passDown(new Event(Event.SUSPEND_OK));
								isBlockState=false;
							}
						} catch (InterruptedException e) {
						}
					}
				}
				break;
		}
		passDown(evt);
	}

	public void up(Event evt) {

		Message msg = null;
		switch (evt.getType()) {
			case Event.MSG:
				msg = (Message) evt.getArg();
				FlushHeader fh = (FlushHeader) msg.removeHeader(getName());
				if (fh != null) {
					if (fh.type == FlushHeader.START_FLUSH) {
						passUp(new Event(Event.BLOCK));
						onFlushStart(msg.getSrc(), fh);
					} else if (fh.type == FlushHeader.STOP_FLUSH) {
						onStopFlush();
					} else if (isCurrentFlushMessage(fh)) {
						if (fh.type == FlushHeader.FLUSH_OK) {
							updateOnFlushOk((Address) msg.getSrc(), fh.viewID);
						} else if (fh.type == FlushHeader.FLUSH_COMPLETED) {
							onFlushCompleted((Address) msg.getSrc());
						}
					} else {
						log.debug(localAddress
								+ " received outdated FLUSH message " + fh
								+ ",ignoring it.");
					}
					return; //do not pass FLUSH msg up
				}
				break;

			case Event.VIEW_CHANGE:
				onViewChange((View) evt.getArg());		
				break;

			case Event.SET_LOCAL_ADDRESS:
				localAddress = (Address) evt.getArg();
				break;

			case Event.SUSPECT:
				onSuspect((Address) evt.getArg());
				break;

			case Event.SUSPEND:				
				onSuspend((View)evt.getArg());
				return;

			case Event.RESUME:				
				onResume();
				return;
		}

		passUp(evt);
	}

	private boolean isCurrentFlushMessage(FlushHeader fh) {
		synchronized (sharedLock) {
			ViewId viewId = currentView.getVid();
			return flushMembers != null && viewId != null
					&& viewId.getId() <= fh.viewID;
		}
	}	
	
	private void onViewChange(View view)
	{
		synchronized (sharedLock) {
			currentView = view;
			
			//If coordinator leaves, its STOP FLUSH message will be discarded by 
			//other members at NAKACK layer. Remaining members will be hung, waiting 
			//for STOP_FLUSH message. If I am new coordinator I will complete the 
			//FLUSH and send STOP_FLUSH on flush callers behalf.
			if (flushCaller != null && !view.getMembers().contains(flushCaller)
					&& localAddress.equals(view.getMembers().get(0))) {
				log.debug("Coordinator left, " + localAddress
						+ " will complete flush");
				onResume();
			}
			
		}	
		log.debug("Installing view at  " + localAddress + " view is "
				+ currentView);
	}

	private void onStopFlush() {
		log.debug("Received STOP_FLUSH at " + localAddress);
		synchronized (blockMutex) {
			isBlockState = false;
			blockMutex.notifyAll();
		}	
	}
	
	private void onSuspend(View view)
	{			
		Message msg = null;
		synchronized(sharedLock)
		{						
			//initiate FLUSH only on group members that we need to flush 
			flushMembers = new ArrayList(view.getMembers());
			flushMembers.retainAll(currentView.getMembers());			
			msg = new Message(null, localAddress, null);
			msg.putHeader(getName(), new FlushHeader(FlushHeader.START_FLUSH,
					currentView.getVid().getId(), flushMembers));
		}
		passDown(new Event(Event.MSG, msg));
		log.debug("Received SUSPEND at " + localAddress
				+ ", sent START_FLUSH to " + flushMembers);
	}
	
	private void onResume()
	{
		Message msg = new Message(null, localAddress, null);
		msg.putHeader(getName(), new FlushHeader(FlushHeader.STOP_FLUSH));
		passDown(new Event(Event.MSG, msg));
		log.debug("Received RESUME at " + localAddress
				+ ", sent STOP_FLUSH to all");
	}

	private void onFlushStart(Address flushStarter, FlushHeader fh) {		
		synchronized (blockMutex) {
			isBlockState = true;
		}
		synchronized (sharedLock) {
			flushCompletedSet.clear();
			flushOkSet.clear();
			flushCaller = flushStarter;
			flushMembers=fh.flushParticipants;
		}
		Message msg = new Message(null, localAddress, null);
		msg.putHeader(getName(), new FlushHeader(FlushHeader.FLUSH_OK,fh.viewID));
		passDown(new Event(Event.MSG, msg));	
		log.debug("Received START_FLUSH at " + localAddress + " responded with FLUSH_OK");
	}
	
	private void updateOnFlushOk(Address address, long viewID) {

		boolean flushOkCompleted = false;
		Message m = null;
		synchronized (sharedLock) {
			flushOkSet.add(address);
			flushOkCompleted = flushOkSet.containsAll(flushMembers);
			if(flushOkCompleted)
			{
				m = new Message(flushCaller,localAddress, null);
			}
		}
		
		log.debug("FLUSH_OK from " + address + ",completed " + flushOkCompleted
				+ ",  flushOkSet " + flushOkSet.toString());

		if (flushOkCompleted) {
			m.putHeader(getName(), new FlushHeader(FlushHeader.FLUSH_COMPLETED,viewID));
			passDown(new Event(Event.MSG, m));
			log.debug(localAddress + " sent FLUSH_COMPLETED message to " +flushCaller);
		}
	}

	private void onFlushCompleted(Address address) {
		boolean flushCompleted = false;
		synchronized (sharedLock) {
			flushCompletedSet.add(address);
			flushCompleted = flushCompletedSet.containsAll(flushMembers);
		}
		
		log.debug("FLUSH_COMPLETED from " + address + ",completed "
				+ flushCompleted + ",flushCompleted "
				+ flushCompletedSet.toString());
		
		if (flushCompleted) {
			passDown(new Event(Event.SUSPEND_OK));			
			log.debug("All FLUSH_COMPLETED received at " + localAddress
					+ " sent SUSPEND_OK down");
		}
	}

	private void onSuspect(Address address) {				
		boolean restartFlush = false;
		long viewID = 0;	
		Collection membersToFlush=null;
		synchronized (sharedLock) {
			viewID = currentView.getVid().getId();
			restartFlush = isFlushCaller(address)
					&& amIFlushCallersNeighbor(address);
			if (restartFlush) {
				membersToFlush = new ArrayList(currentView.getMembers());
				membersToFlush.remove(address);
			}
		}
		if (restartFlush) {				
			Message msg = new Message(null, localAddress, null);
			msg.putHeader(getName(), new FlushHeader(FlushHeader.START_FLUSH,viewID,membersToFlush));
			passDown(new Event(Event.MSG, msg));
			log.debug("SUSPECT was FLUSH caller. " + localAddress
					+ " is neighbor that restarted FLUSH ");
		}		
	}
	
	private boolean isFlushCaller(Address address)
	{
		return address.equals(flushCaller);		
	}
	
	private boolean amIFlushCallersNeighbor(Address address)
	{		
		boolean amINeighbor = false;
		if(currentView.size()>1)
		{			
			Vector members = currentView.getMembers();
			int suspectsIndex = members.indexOf(address);
			boolean isLast = members.size()==suspectsIndex+1;
			if(isLast)
			{
				amINeighbor = localAddress.equals(currentView.getCreator());
			}
			else
			{
				amINeighbor = localAddress.equals(members.get(suspectsIndex+1));
			}
		}
		return amINeighbor;
	}

	private boolean isFlushRunning() {
		return isBlockState;
	}	

	public static class FlushHeader extends Header {
		public static final byte START_FLUSH = 0;

		public static final byte FLUSH_OK = 1;

		public static final byte STOP_FLUSH = 2;
		
		public static final byte FLUSH_COMPLETED = 3;

		byte type;
		
		long viewID;
		
		Collection flushParticipants;

		public FlushHeader() {
			this(START_FLUSH);
		} // used for externalization

		public FlushHeader(byte type) {
			this(type,0);
		}
		
		public FlushHeader(byte type,long viewID) {
			this(type,viewID,null);
		}
		public FlushHeader(byte type,long viewID,Collection flushView) {
			this.type = type;
			this.viewID = viewID;
			this.flushParticipants = flushView;
		}

		public String toString() {
			switch (type) {
				case START_FLUSH:
					return "FLUSH[type=START_FLUSH,viewId=" + viewID
							+ ",members=" + flushParticipants + "]";
				case FLUSH_OK:
					return "FLUSH[type=FLUSH_OK,viewId=" + viewID + "]";
				case STOP_FLUSH:
					return "FLUSH[type=STOP_FLUSH]";
				case FLUSH_COMPLETED:
					return "FLUSH[type=FLUSH_COMPLETED,viewId=" + viewID + "]";
				default:
					return "[FLUSH: unknown type (" + type + ")]";
			}
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeByte(type);	
			out.writeLong(viewID);
			out.writeObject(flushParticipants);
		}

		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			type = in.readByte();
			viewID = in.readLong();
			flushParticipants = (Collection) in.readObject();
		}		
	}
}
