package org.jgroups;

/**
 * Base class for JGroups exceptions.
 * 
 * @since  5.5
 */
public class JGroupsException extends RuntimeException {

    private static final long serialVersionUID = -1357346823441443746L;

    public JGroupsException(String message) {
        super(message);
    }

    public JGroupsException(Throwable cause) {
        super(cause);
    }
}
