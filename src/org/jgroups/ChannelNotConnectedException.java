// $Id: ChannelNotConnectedException.java,v 1.2 2005/07/17 11:38:05 chrislott Exp $

package org.jgroups;

/**
 * Thrown if an operation is attemped on an unconnected channel.
 */
public class ChannelNotConnectedException extends ChannelException {

    public ChannelNotConnectedException() {
    }

    public ChannelNotConnectedException(String reason) {
        super(reason);
    }

    public String toString() {
        return "ChannelNotConnectedException";
    }
}
