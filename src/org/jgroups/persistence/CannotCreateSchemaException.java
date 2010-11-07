package  org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * This exception inherits the Exception class and is used in
 * cases where the Persistence Storage cannot create schema to
 * use the API as required. At this point, top level use needs to
 * decide whether to continue or abort.
 */

public class CannotCreateSchemaException extends Exception
{

    private static final long serialVersionUID = 291582260022140141L;
	
    /**
     * @param t
     * @param reason implementor-specified runtime reason
     */
    public CannotCreateSchemaException(Throwable t, String reason)
    {
	this.t = t;
	this.reason = reason;
    }

    /**
     * @return String
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
    private Throwable t = null;
    private String reason = null;
}
