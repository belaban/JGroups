// $Id: ChannelException.java,v 1.2 2004/07/30 04:42:13 jiwils Exp $

package org.jgroups;


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

    public String toString() {
        return "ChannelException: " + getMessage();
    }
}
