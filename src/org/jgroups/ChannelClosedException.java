// $Id: ChannelClosedException.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $

package org.jgroups;

/**
 * Thrown if an operation is attemped on a closed channel.
 */
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
