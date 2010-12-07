package org.jgroups.protocols;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.jgroups.Global;
import org.jgroups.Header;

/**
 * This Header class is used in conjuction with the PRIO protocol to priorize message sending/receiving
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
public class PrioHeader extends Header {
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

	public void writeTo( DataOutputStream outstream ) throws IOException {
		outstream.writeByte(priority);
	}

	public void readFrom( DataInputStream instream ) throws IOException, IllegalAccessException, InstantiationException {
		priority=instream.readByte();
	}

	@Override
	public String toString()
	{
		return "PRIO priority=" + priority;
	}
}
