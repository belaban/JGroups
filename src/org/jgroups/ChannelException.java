// $Id: ChannelException.java,v 1.4 2004/08/04 14:26:34 belaban Exp $

package org.jgroups;

import java.io.PrintStream;
import java.io.PrintWriter;

import java.util.StringTokenizer;

/**
 * This class represents the super class for all exception types thrown by
 * JGroups.
 */
public class ChannelException extends Exception {
    // Class-level implementation.
    private static final boolean IS_JAVA_13;

    static {
        // Check to see if we are in a 1.3 VM.  If so, we need to change how
        // we print stack traces.
        String javaSpecVersion=
                System.getProperty("java.specification.version");

        StringTokenizer tokenizer=new StringTokenizer(javaSpecVersion, ".");

        int majorVersion=Integer.parseInt(tokenizer.nextToken());
        int minorVersion=Integer.parseInt(tokenizer.nextToken());

        if(majorVersion == 1 && minorVersion == 3) {
            IS_JAVA_13=true;
        }
        else {
            IS_JAVA_13=false;
        }
    }

    // Instance-level implementation.
    private Throwable _cause;

    public ChannelException() {
        super();
    }

    public ChannelException(String reason) {
        super(reason);
    }

    public ChannelException(String reason, Throwable cause) {
        super(reason);
        _cause = cause;
    }

    public String toString() {
        return "ChannelException: " + getMessage();
    }

    /**
     * Retrieves the cause of this exception as passed to the constructor.
     * <p>
     * This method is provided so that in the case that a 1.3 VM is used,
     * 1.4-like exception chaining functionality is possible.  If a 1.4 VM is
     * used, this method will override <code>Throwable.getCause()</code> with a
     * version that does exactly the same thing.
     *
     * @return the cause of this exception.
     */
	public Throwable getCause() {
		return _cause;
	}

    /*
     * Throwable implementation.
     */

    /**
     * Prints this exception's stack trace to standard error.
     * <p>
     * This method is provided so that in the case that a 1.3 VM is used, calls
     * to <code>printStackTrace</code> can be intercepted so that 1.4-like
     * exception chaining functionality is possible.
     */
    public void printStackTrace() {
        printStackTrace(System.err);
    }

    /**
     * Prints this exception's stack trace to the provided stream.
     * <p>
     * This method implements the 1.4-like exception chaining functionality when
     * printing stack traces for 1.3 VMs.  If a 1.4 VM is used, this call is
     * delegated only to the super class.
     *
     * @param ps the stream to which the stack trace will be "printed".
     */
    public void printStackTrace(PrintStream ps) {
        synchronized (ps) {
            super.printStackTrace(ps);

            if (IS_JAVA_13) {
                printCauseStackTrace(ps);
            }
        }
    }

    /**
     * Prints this exception's stack trace to the provided writer.
     * <p>
     * This method implements the 1.4-like exception chaining functionality when
     * printing stack traces for 1.3 VMs.  If a 1.4 VM is used, this call is
     * delegated only to the super class.
     *
     * @param pw the writer to which the stack trace will be "printed".
     */
    public void printStackTrace(PrintWriter pw) {
        synchronized (pw) {
            super.printStackTrace(pw);

            if (IS_JAVA_13) {
                printCauseStackTrace(pw);
            }
        }
    }

    private void printCauseStackTrace(PrintStream ps) {
        if (_cause != null) {
            ps.print("Caused by: ");
            _cause.printStackTrace(ps);
        }
    }

    private void printCauseStackTrace(PrintWriter pw) {
        if (_cause != null) {
            pw.print("Caused by: ");
            _cause.printStackTrace(pw);
        }
    }
}
