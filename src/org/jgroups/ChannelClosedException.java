// $Id: ChannelClosedException.java,v 1.2 2003/11/27 21:36:46 belaban Exp $

package org.jgroups;


public class ChannelClosedException extends ChannelException {

    public ChannelClosedException() {
        super();
    }

    public ChannelClosedException(String msg) {
        super(msg);
    }

    public String toString() {
        return "ChannelClosedException";
    }
}
