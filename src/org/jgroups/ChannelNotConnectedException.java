// $Id: ChannelNotConnectedException.java,v 1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;


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
