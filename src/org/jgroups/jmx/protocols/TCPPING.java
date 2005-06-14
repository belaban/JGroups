package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: TCPPING.java,v 1.1 2005/06/14 10:10:10 belaban Exp $
 */
public class TCPPING extends Discovery implements TCPPINGMBean {

    public TCPPING() {
    }

    public TCPPING(org.jgroups.stack.Protocol p) {
        super(p);
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
    }
}
