package org.jgroups.protocols.pbcast;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;

public class FLUSH extends Protocol {

	public static final String NAME = "FLUSH";

	private View currentView;

	Address localAddress = null;

	Set flushOkSet;
	
	List flushCompleted;

	private boolean isFlushRunning;
	
	private final Object blockMutex=new Object();

	private boolean isBlockState = false;

	private long timeout = 3000;

	public FLUSH() {
		super();
		flushOkSet = new TreeSet();
		flushCompleted = new ArrayList();
	}

	public String getName() {
		return NAME;
	}

	public void down(Event evt) {
		synchronized(blockMutex) {
            while(isBlockState) {
                try {
                    blockMutex.wait();
                }
                catch(InterruptedException e) {
                }
            }
        }
		super.down(evt);
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
			log.error("FD.setProperties(): the following properties are not recognized: "
							+ props);

			return false;
		}
		return true;
	}

	public void start() throws Exception {
		// TODO Auto-generated method stub
		super.start();
	}

	public void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	public void up(Event evt) {

		Message msg = null;
		switch (evt.getType()) {
			case Event.MSG:
				msg = (Message) evt.getArg();
				FlushHeader fh = (FlushHeader) msg.removeHeader(getName());
				if (fh != null) {
					switch (fh.type) {
						case FlushHeader.START_FLUSH:
							passUp(new Event(Event.BLOCK));
							onFlushStart();
							return;

						case FlushHeader.FLUSH_OK:							
							updateOnFlushOk((Address) msg.getSrc());							
							return;
						case FlushHeader.STOP_FLUSH:
							synchronized (blockMutex) {
								isBlockState=false;
								blockMutex.notifyAll();
							}
							return;
						case FlushHeader.FLUSH_COMPLETED:
							//only coordinator receives this message
							onFlushCompleted((Address) msg.getSrc());
							return;
							
					}
				}
				break;

			case Event.VIEW_CHANGE:
				currentView = (View) evt.getArg();
				break;

			case Event.SET_LOCAL_ADDRESS:
				localAddress = (Address) evt.getArg();
				break;

			case Event.SUSPECT:
				onSuspect((Address) evt.getArg());
				break;

			case Event.SUSPEND:
				isFlushRunning = true;
				msg = new Message(null, localAddress, null);
				msg.putHeader(getName(), new FlushHeader(
						FlushHeader.START_FLUSH));
				passDown(new Event(Event.MSG, msg));
				return;

			case Event.RESUME:				
				isFlushRunning = false;
				msg = new Message(null, localAddress, null);
				msg.putHeader(getName(),
						new FlushHeader(FlushHeader.STOP_FLUSH));
				passDown(new Event(Event.MSG, msg));
				return;
		}

		passUp(evt); // pass up to the layer above us
	}

	private void onFlushStart() {
		synchronized (blockMutex) {
			isBlockState=true;
		}
		
		flushCompleted.clear();
		flushOkSet.clear();
		Message msg = new Message(null, localAddress, null);
		msg.putHeader(getName(), new FlushHeader(
				FlushHeader.FLUSH_OK));
		passDown(new Event(Event.MSG, msg));
	}

	private void onFlushCompleted(Address address) {
		flushCompleted.add(address);
		if (flushCompleted.size() >= currentView.size())
		{
			passDown(new Event(Event.SUSPEND_OK));
		}
	}

	private void onSuspect(Address address) {
		if (isFlushRunning()) {
			updateOnFlushOk(address);
			
			if (isCoordinator(address) && amISecondMemberInView())
			{
				//restart flush
				isFlushRunning = true;
				Message msg = new Message(null, localAddress, null);
				msg.putHeader(getName(), new FlushHeader(
						FlushHeader.START_FLUSH));
				passDown(new Event(Event.MSG, msg));
			}
		}
	}
	
	private boolean isCoordinator(Address address)
	{
		return address.equals(currentView.getMembers().firstElement());
	}
	
	private boolean amISecondMemberInView()
	{
		Address secondMemnerInView = (Address) currentView.getMembers().get(1);
		return localAddress.equals(secondMemnerInView);
	}

	private boolean isFlushRunning() {
		return isFlushRunning;
	}

	private void updateOnFlushOk(Address address) {

		flushOkSet.add(address);
		if (flushOkSet.size() >= currentView.size()) {
			Message m = new Message((Address) currentView.getMembers().firstElement(),
					localAddress, null);
			m.putHeader(getName(), new FlushHeader(FlushHeader.FLUSH_COMPLETED));
			passDown(new Event(Event.MSG, m));
		}
	}

	public static class FlushHeader extends Header implements Streamable {
		public static final byte START_FLUSH = 0;

		public static final byte FLUSH_OK = 1;

		public static final byte STOP_FLUSH = 2;
		
		public static final byte FLUSH_COMPLETED = 3;

		byte type;

		public FlushHeader() {
			this(START_FLUSH);
		} // used for externalization

		public FlushHeader(byte type) {
			this.type = type;
		}

		public String toString() {
			switch (type) {
				case START_FLUSH:
					return "[FLUSH:start flush]";
				case FLUSH_OK:
					return "[FLUSH:flush ok]";
				case STOP_FLUSH:
					return "[FLUSH:stop flush";
				case FLUSH_COMPLETED:
					return "[FLUSH:flush completed]";										
				default:
					return "[FLUSH: unknown type (" + type + ")]";
			}
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeByte(type);
		}

		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			type = in.readByte();
		}

		public long size() {
			return Global.BYTE_SIZE; // type
		}

		public void writeTo(DataOutputStream out) throws IOException {
			out.writeByte(type);
		}

		public void readFrom(DataInputStream in) throws IOException,
				IllegalAccessException, InstantiationException {
			type = in.readByte();
		}
	}
}
