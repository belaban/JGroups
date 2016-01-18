package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.AbstractHeader;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * This Header class is used in conjunction with the PRIO protocol to prioritize message sending/receiving
 * Priority values are from 0 to 255 where 0 is the highest priority
 *
 * Example of setting a message priority:
 * <code>
 *  // Create a message to send to everyone
 *	Message message = new Message( null, null, messagePayload );
 *	// Add the priority protocol header
 *	PrioHeader header = new PrioHeader( 1 );
 *	short protocolId = ClassConfigurator.getProtocolId(PRIO.class);
 *	message.putHeader( protocolId, header);
 * </code>
 * @author Michael Earl
 */
public class PrioHeader extends AbstractHeader {
	private byte priority = 0;

	public PrioHeader() {
	}

	public PrioHeader( byte priority ) {
		this.priority = priority;
	}

	public byte getPriority() {
		return priority;
	}

	public void setPriority( byte priority ) {
		this.priority = priority;
	}

	@Override
	public int size() {
		return Global.BYTE_SIZE;
	}

	public void writeTo( DataOutput outstream ) throws Exception {
		outstream.writeByte(priority);
	}

	public void readFrom( DataInput instream ) throws Exception {
		priority=instream.readByte();
	}

	@Override
	public String toString()
	{
		return "PRIO priority=" + priority;
	}
}
