
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
