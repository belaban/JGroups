package org.jgroups.protocols;

import org.jgroups.blocks.ConnectionTableNIO;
import org.jgroups.blocks.ConnectionTable;

import java.net.InetAddress;

public class TCP_NIO extends TCP
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgroups.protocols.TCP#getConnectionTable(long, long)
	 */
	protected ConnectionTable getConnectionTable(long ri, long cet,
			InetAddress b_addr, InetAddress bc_addr, int s_port, int e_port) throws Exception {
		ConnectionTableNIO ct = null;
		if (ri == 0 && cet == 0) {
			ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port);
		} else {
			if (ri == 0) {
				ri = 5000;
				if(log.isWarnEnabled()) log.warn("reaper_interval was 0, set it to "
						+ ri);
			}
			if (cet == 0) {
				cet = 1000 * 60 * 5;
				if(log.isWarnEnabled()) log.warn("conn_expire_time was 0, set it to "
						+ cet);
			}
			ct = new ConnectionTableNIO(this, b_addr, bc_addr, s_port, e_port, ri, cet);
		}
		return ct;
	}
	
	public String getName() {
        return "TCP_NIO";
    }
}