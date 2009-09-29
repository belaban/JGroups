package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * <p>
 * This is an example of using a preshared token that is encrypted using an MD5/SHA hash for authentication purposes.  All members of the group have to have the same string value in the JGroups config.
 *</p>
 * <p>
 * Configuration parameters for this example are shown below:
 * </p>
 * <ul>
 *  <li>token_hash (required) = MD5(default)/SHA</li>
 *  <li>auth_value (required) = the string to encrypt</li>
 * </ul>
 * @see org.jgroups.auth.AuthToken
 * @author Chris Mills
 */
public class MD5Token extends AuthToken {

    @Property
    private String auth_value= null;

    @Property(name="token_hash")
    private String hash_type = "MD5";
    private static final long serialVersionUID=-5787154335375249191L;

    public MD5Token(){
        //need an empty constructor
    }

    public MD5Token(String authvalue){
        this.auth_value= hash(authvalue);
    }

    public MD5Token(String authvalue, String hash_type){
        this.auth_value= hash(authvalue);
        this.hash_type = hash_type;
    }


    public String getHashType() {
        return hash_type;
    }

    public void setHashType(String hash_type) {
        this.hash_type=hash_type;
    }

    public String getAuthValue() {
        return auth_value;
    }

    public void setAuthValue(String auth_value) {
        this.auth_value=auth_value;
    }

    public String getName(){
        return "org.jgroups.auth.MD5Token";
    }

  

    /**
     * Called during setup to hash the auth_value string in to an MD5/SHA hash
     * @param token the string to hash
     * @return the hashed version of the string
     */
    private String hash(String token){
        //perform the hashing of the token key
        String hashedToken = null;

        if(hash_type.equalsIgnoreCase("SHA")){
            hashedToken = Util.sha(token);
        }else{
            hashedToken = Util.md5(token);
        }

        if(hashedToken == null){
            //failed to encrypt
            if(log.isWarnEnabled()){
                log.warn("Failed to hash token - sending in clear text");
            }
            return token;
        }
        return hashedToken;
    }

    public boolean authenticate(AuthToken token, Message msg){

        if((token != null) && (token instanceof MD5Token)){
            //Found a valid Token to authenticate against
            MD5Token serverToken = (MD5Token) token;

            if((this.auth_value != null) && (serverToken.auth_value != null) && (this.auth_value.equalsIgnoreCase(serverToken.auth_value))){
                //validated
                if(log.isDebugEnabled()){
                    log.debug("MD5Token match");
                }
                return true;
            }else{
//                if(log.isWarnEnabled()){
//                    log.warn("Authentication failed on MD5Token");
//                }
                return false;
            }
        }

        if(log.isWarnEnabled()){
            log.warn("Invalid AuthToken instance - wrong type or null");
        }
        return false;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        if(log.isDebugEnabled()){
            log.debug("MD5Token writeTo()");
        }
        Util.writeString(this.auth_value, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        if(log.isDebugEnabled()){
            log.debug("MD5Token readFrom()");
        }
        this.auth_value= Util.readString(in);
    }
}
