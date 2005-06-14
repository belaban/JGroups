package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: TCPGOSSIP.java,v 1.1 2005/06/14 10:10:10 belaban Exp $
 */
public class TCPGOSSIP extends Discovery implements TCPGOSSIPMBean {

    public TCPGOSSIP() {
    }

    public TCPGOSSIP(org.jgroups.stack.Protocol p) {
        super(p);
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
    }
}
