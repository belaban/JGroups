// $Id: ChannelException.java,v 1.5 2005/05/30 13:50:43 belaban Exp $

package org.jgroups;

import java.io.PrintStream;
import java.io.PrintWriter;

import java.util.StringTokenizer;

/**
 * This class represents the super class for all exception types thrown by
 * JGroups.
 */
public class ChannelException extends Exception {

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
