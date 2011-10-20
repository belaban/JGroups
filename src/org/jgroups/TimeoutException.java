
package org.jgroups;

/**
 * Thrown if members fail to respond in time.
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class TimeoutException extends RuntimeException {

    private static final long serialVersionUID=-2928348023967934826L;

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
