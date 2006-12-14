package org.jgroups.jmx.protocols;

/**
 * @author Bela Ban
 * @version $Id: TCPPING.java,v 1.2 2006/12/14 07:46:59 belaban Exp $
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
