package org.jgroups.protocols;

import java.net.InetAddress;

import org.jgroups.blocks.ConnectionTable1_4;
import org.jgroups.log.Trace;

public class TCP1_4 extends TCP
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgroups.protocols.TCP#initConnectionTable1_4(long, long)
	 */
	protected ConnectionTable1_4 getConnectionTable1_4(long ri, long cet,
			InetAddress b_addr, int s_port) throws Exception {
		ConnectionTable1_4 ct = null;
		if (ri == 0 && cet == 0) {
			ct = new ConnectionTable1_4(this, b_addr, s_port);
		} else {
			if (ri == 0) {
				ri = 5000;
				Trace.warn("TCP.start()", "reaper_interval was 0, set it to "
						+ ri);
			}
			if (cet == 0) {
				cet = 1000 * 60 * 5;
				Trace.warn("TCP.start()", "conn_expire_time was 0, set it to "
						+ cet);
			}
			ct = new ConnectionTable1_4(this, b_addr, s_port, ri, cet);
		}
		return ct;
	}
	
	public String getName() {
        return "TCP1_4";
    }
}