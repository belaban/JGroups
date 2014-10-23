package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * <p>
 * This is an example of using a preshared token for authentication purposes. All members of the
 * group have to have the same string value in the JGroups config.
 * </p>
 * <p>
 * JGroups config parameters:
 * </p>
 * <ul>
 * <li>auth_value (required) = the string to encrypt</li>
 * </ul>
 * 
 * @author Chris Mills
 * @see org.jgroups.auth.AuthToken
 */
public class SimpleToken extends AuthToken {

    @Property(exposeAsManagedAttribute=false)
    private String auth_value = null;

    public SimpleToken() { // need an empty constructor
    }

    public SimpleToken(String authvalue) {
        this.auth_value = authvalue;
    }

    public String getName() {
        return "org.jgroups.auth.SimpleToken";
    }

    public String getAuthValue() {
        return auth_value;
    }

    public void setAuthValue(String auth_value) {
        this.auth_value = auth_value;
    }

    public boolean authenticate(AuthToken token, Message msg) {
        if ((token != null) && (token instanceof SimpleToken)) {
            // Found a valid Token to authenticate against
            SimpleToken serverToken = (SimpleToken) token;

            if ((this.auth_value != null) && (serverToken.auth_value != null)
                            && (this.auth_value.equalsIgnoreCase(serverToken.auth_value))) {
                // validated
                if (log.isDebugEnabled()) {
                    log.debug("SimpleToken match");
                }
                return true;
            } else {
                // if(log.isWarnEnabled()){
                // log.warn("Authentication failed on SimpleToken");
                // }
                return false;
            }
        }

        if (log.isWarnEnabled()) {
            log.warn("Invalid AuthToken instance - wrong type or null");
        }
        return false;
    }

    /**
     * Required to serialize the object to pass across the wire
     * 
     *
     *
     * @param out
     * @throws Exception
     */
    public void writeTo(DataOutput out) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("SimpleToken writeTo()");
        }
        Util.writeString(this.auth_value, out);
    }

    /**
     * Required to deserialize the object when read in from the wire
     * 
     *
     *
     * @param in
     * @throws Exception
     */
    public void readFrom(DataInput in) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("SimpleToken readFrom()");
        }
        this.auth_value = Util.readString(in);
    }

    public int size() {
        return Util.size(auth_value);
    }

    public String toString() {
        return "auth_value=" + auth_value;
    }
}