package org.jgroups.protocols;

import org.jgroups.blocks.ConnectionTable1_4;

import java.net.InetAddress;

public class TCP1_4 extends TCP
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jgroups.protocols.TCP#initConnectionTable1_4(long, long)
	 */
	protected ConnectionTable1_4 getConnectionTable1_4(long ri, long cet,
			InetAddress b_addr, InetAddress bc_addr, int s_port, int e_port) throws Exception {
		ConnectionTable1_4 ct = null;
		if (ri == 0 && cet == 0) {
			ct = new ConnectionTable1_4(this, b_addr, bc_addr, s_port, e_port);
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
			ct = new ConnectionTable1_4(this, b_addr, bc_addr, s_port, e_port, ri, cet);
		}
		return ct;
	}
	
	public String getName() {
        return "TCP1_4";
    }
}