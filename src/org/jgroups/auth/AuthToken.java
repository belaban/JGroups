package org.jgroups.auth;

import org.jgroups.util.Streamable;
import org.jgroups.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Properties;
/**
 * Abstract AuthToken class used by implementations of AUTH, e.g. SimpleToken, X509Token
 * @author Chris Mills
 */
public abstract class AuthToken implements Serializable, Streamable{
    protected final Log log = LogFactory.getLog(this.getClass());
    
    /**
     * Used to return the full package and class name of the implementation.  This is used by the AUTH protocol to create an instance of the implementation.
     * @return a java.lang.String object of the package and class name
     */
    public abstract String getName();

    /**
     * This method should be implemented to perform the actual authentication of joining members.
     * @param token the token sent by the joiner
     * @param msg the Message object containing the actual JOIN_REQ
     * @return true if authenticaion passed or false if it failed.
     */
    public abstract boolean authenticate(AuthToken token, Message msg);
}
