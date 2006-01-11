package org.jgroups.auth;

import org.jgroups.util.Streamable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.Properties;
/**
 * Abstract AuthToken class used by implementations of AUTH, e.g. SimpleToken, X509Token1_5
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
     * Called during the setup of the AUTH protocol to pass property values from the JGroups config XML document to the implementing class.
     * @param properties a java.util.Properties object of config parameters
     */
    public abstract void setValue(Properties properties);
    /**
     * This method should be implemented to perform the actual authentication of joining members.
     * @param token the token sent by the joiner
     * @return true if authenticaion passed or false if it failed.
     */
    public abstract boolean authenticate(AuthToken token);
}
