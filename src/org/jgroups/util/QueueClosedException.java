// $Id: QueueClosedException.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

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
            return "QueueClosedException:" + this.getMessage();
        else
            return "QueueClosedException";
    }
}
