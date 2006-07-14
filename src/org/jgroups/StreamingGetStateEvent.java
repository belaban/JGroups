package org.jgroups;
/**
 * 
 * TODO
 */
import java.io.OutputStream;

public class StreamingGetStateEvent {

	OutputStream os;
	
	public StreamingGetStateEvent(OutputStream os) {
		super();
		this.os=os;
	}
	
	public OutputStream getArg()
	{
		return os;
	}

}
