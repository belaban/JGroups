package org.jgroups;

/**
 * Adds additional methods for partial state transfer (http://jira.jboss.com/jira/browse/JGRP-118). The state_id
 * identifies the substate an application is to return or set. This interface will be merged with MessageListener
 * in 3.0 (API changes)
 * @author Bela Ban
 * @version $Id: ExtendedMessageListener.java,v 1.1 2006/03/16 09:56:28 belaban Exp $
 */
public interface ExtendedMessageListener extends MessageListener {
    byte[] getState(String state_id);
    void setState(String state_id, byte[] state);
}
