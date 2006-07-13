// $Id: QueueClosedException.java,v 1.2 2006/07/13 07:16:14 belaban Exp $

package org.jgroups.util;


public class QueueClosedException extends Exception {

    public QueueClosedException() {

    }

    public QueueClosedException( String msg )
    {
        super( msg );
    }

    public String toString() {
        if ( this.getMessage() != null )
            return "QueueClosedException: " + this.getMessage();
        else
            return "QueueClosedException";
    }
}
