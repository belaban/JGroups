package org.jgroups.auth;

import org.jgroups.util.Util;
import org.jgroups.Message;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.util.Properties;
/**
 * <p>
 * This is an example of using a preshared token for authentication purposes.  All members of the group have to have the same string value in the JGroups config.
 * </p>
 * <p>JGroups config parameters:</p>
 *<ul>
 *  <li>auth_value (required) = the string to encrypt</li>
 * </ul>
 * @see org.jgroups.auth.AuthToken
 * @author Chris Mills
 */
public class SimpleToken extends AuthToken {

    public static final String TOKEN_ATTR = "auth_value";
    private String token = null;
    private static final long serialVersionUID=-6889408182212567295L;

    public SimpleToken(){
        //need an empty constructor
    }

    public SimpleToken(String token){
        this.token = token;
    }

    public void setValue(Properties properties){
        this.token = (String)properties.get(SimpleToken.TOKEN_ATTR);
        properties.remove(SimpleToken.TOKEN_ATTR);
    }

    public String getName(){
        return "org.jgroups.auth.SimpleToken";
    }

    public boolean authenticate(AuthToken token, Message msg){
        if((token != null) && (token instanceof SimpleToken)){
            //Found a valid Token to authenticate against
            SimpleToken serverToken = (SimpleToken) token;

            if((this.token != null) && (serverToken.token != null) && (this.token.equalsIgnoreCase(serverToken.token))){
                //validated
                if(log.isDebugEnabled()){
                    log.debug("SimpleToken match");
                }
                return true;
            }else{
                if(log.isWarnEnabled()){
                    log.warn("Authentication failed on SimpleToken");
                }
                return false;
            }
        }

        if(log.isWarnEnabled()){
            log.warn("Invalid AuthToken instance - wrong type or null");
        }
        return false;
    }
    /**
     * Required to serialize the object to pass across the wire
     * @param out
     * @throws IOException
     */
    public void writeTo(DataOutputStream out) throws IOException {
        if(log.isDebugEnabled()){
            log.debug("SimpleToken writeTo()");
        }
        Util.writeString(this.token, out);
    }
    /**
     * Required to deserialize the object when read in from the wire
     * @param in
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        if(log.isDebugEnabled()){
            log.debug("SimpleToken readFrom()");
        }
        this.token = Util.readString(in);
    }
}
