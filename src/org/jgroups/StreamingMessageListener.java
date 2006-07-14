package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 *TODO
 */
public interface StreamingMessageListener extends MessageListener {
	
	public void getState(OutputStream ostream);
	public void setState(InputStream istream);

}
