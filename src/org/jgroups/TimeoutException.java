
package org.jgroups;

/**
 * Thrown if members fail to respond in time.
 */
public class TimeoutException extends Exception {
    private static final long serialVersionUID = -3555655828017487825L;

    public TimeoutException() {
        super("TimeoutException");
    }

    public TimeoutException(String msg) {
        super(msg);
    }


    public String toString() {
        return super.toString();
    }
}
