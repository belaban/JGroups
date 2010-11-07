// $Id: ChannelClosedException.java,v 1.4 2006/11/13 17:42:11 bstansberry Exp $

package org.jgroups;

/**
 * Thrown if an operation is attemped on a closed channel.
 */
public class ChannelClosedException extends ChannelException {

    private static final long serialVersionUID = -5172168752255182905L;

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
