package org.jgroups.tests.perf;

/**
 * @author Bela Ban Jan 22
 * @author 2004
 * @version $Id: Receiver.java,v 1.1 2004/01/23 00:08:31 belaban Exp $
 */
public interface Receiver {
    void receive(Object sender, byte[] payload);
}
