package  org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * This exception inherits the Exception class and is used in
 * cases where the Persistence Storage cannot remove a pair
 * from its storage mechanism (leading to permannt errors)
 */

public class CannotRemoveException extends Exception
{

    /**
     * @param Throwable;
     * @param String; implementor filling in runtime reason
     */
    public CannotRemoveException(Throwable t, String reason)
    {
	this.t = t;
	this.reason = reason;
    }

    /**
     * @return String;
     */
    public String toString()
    {
	String tmp = "Exception " + t.toString() + " was thrown due to " + reason;
	return tmp;
    }

    /**
     * members are made available so that the top level user can dump
     * appropriate members on to his stack trace
     */
    public Throwable t = null;
    public String reason = null;
}
