package org.jgroups.util;

/**
 * Exception raised when a threadpool rejects jobs but shows no progress.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @since 1/30/13
 */
public class NoProgressException extends RuntimeException {
    public NoProgressException(String message) {
        super(message);
    }
}
