// $Id: ChannelClosedException.java,v 1.1.1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;


public class ChannelClosedException extends ChannelException {

    public ChannelClosedException() {
        super();
    }

    public ChannelClosedException(String msg) {
        super(msg);
    }

    public String toString() {
        return "ChannelClosed";
    }
}
