// $Id: ChannelException.java,v 1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;


public class ChannelException extends Exception {

    public ChannelException() {
        super();
    }

    public ChannelException(String reason) {
        super(reason);
    }

    public String toString() {
        return "ChannelException: " + getMessage();
    }
}
