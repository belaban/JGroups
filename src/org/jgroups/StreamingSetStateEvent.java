package org.jgroups;
/**
 * 
 * TODO
 */
import java.io.InputStream;

public class StreamingSetStateEvent {

	InputStream is;
	
	public StreamingSetStateEvent(InputStream is) {
		super();
		this.is=is;
	}
	
	public InputStream getArg()
	{
		return is;
	}

}
