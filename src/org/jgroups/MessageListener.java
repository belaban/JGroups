// $Id: MessageListener.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;



public interface MessageListener {
    void          receive(Message msg);
    byte[]        getState();
    void          setState(byte[] state);
}
