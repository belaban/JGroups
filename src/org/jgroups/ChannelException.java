// $Id: ChannelException.java,v 1.7 2006/11/13 17:42:11 bstansberry Exp $

package org.jgroups;

/**
 * This class represents the super class for all exception types thrown by
 * JGroups.
 */
public class ChannelException extends Exception {

    private static final long serialVersionUID = 6041194633384856098L;

	public ChannelException() {
        super();
    }

    public ChannelException(String reason) {
        super(reason);
    }

    public ChannelException(String reason, Throwable cause) {
        super(reason, cause);
    }

}
