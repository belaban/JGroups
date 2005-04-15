package org.jgroups;

/**
 * @author Bela Ban Mar 29, 2004
 * @version $Id: Global.java,v 1.3 2005/04/15 12:37:19 belaban Exp $
 */
public class Global {
    /** Allows for conditional compilation, e.g. if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info(...) would be removed from the code
	(if recompiled) when this flag is set to false. Therefore, code that should be removed from the final
	product should use if(log.isTraceEnabled()) rather than .
    */
    public static final boolean debug=false;

    /**
     * Used to determine whether to copy messages (copy=true) in retransmission tables,
     * or whether to use references (copy=false). Once copy=false has worked for some time, this flag
     * will be removed entirely
     */
    public static final boolean copy=false;
    

    public static final int BYTE_SIZE  = 1;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE   = 4;
    public static final int LONG_SIZE  = 8;
}
