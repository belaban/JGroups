package org.jgroups.auth;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * <p>
 * This is an example of using a preshared token that is encrypted using an MD5/SHA hash for
 * authentication purposes. All members of the group have to have the same string value in the
 * JGroups config.
 *</p>
 * <p>
 * Configuration parameters for this example are shown below:
 * </p>
 * <ul>
 * <li>token_hash (required) = MD5(default)/SHA</li>
 * <li>auth_value (required) = the string to encrypt</li>
 * </ul>
 *
 * @see org.jgroups.auth.AuthToken
 * @author Chris Mills
 */
public class MD5Token extends AuthToken {

    @Property(exposeAsManagedAttribute=false)
    private String auth_value = null;

    @Property(name = "token_hash")
    private String hash_type = "MD5";

    public MD5Token() {
        // need an empty constructor
    }

    public MD5Token(String authvalue) {
        this.auth_value = hash(authvalue);
    }

    public MD5Token(String authvalue, String hash_type) {
        this.hash_type = hash_type;
        this.auth_value = hash(authvalue);
    }

    public String getHashType() {
        return hash_type;
    }

    public void setHashType(String hash_type) {
        this.hash_type = hash_type;
    }

    public String getAuthValue() {
        return auth_value;
    }

    public void setAuthValue(String auth_value) {
        this.auth_value = auth_value;
    }

    public void hashAndSetAuthValue(String authvalue) {
      this.auth_value = hash(authvalue);
    }

    public String getName() {
        return "org.jgroups.auth.MD5Token";
    }

    /**
     * Called during setup to hash the auth_value string in to an MD5/SHA hash
     *
     * @param token
     *            the string to hash
     * @return the hashed version of the string
     */
    private String hash(String token) {
        // perform the hashing of the token key
        String hashedToken = null;

        if (hash_type.equalsIgnoreCase("SHA")) {
            hashedToken = Util.sha(token);
        } else {
            hashedToken = Util.md5(token);
        }

        if (hashedToken == null) {
            // failed to hash - sending the token in clear text
            // Note that this may be considered a security vulnerabiltiy if clear text passwords are forbidden.

            log.warn("Failed to hash token - sending in clear text");
            return token;
        }
        return hashedToken;
    }

    public boolean authenticate(AuthToken token, Message msg) {

        if ((token != null) && (token instanceof MD5Token)) {
            // Found a valid Token to authenticate against
            MD5Token serverToken = (MD5Token) token;

            // Compare the hash values
            return (this.auth_value != null) && (serverToken.auth_value != null)
              && (this.auth_value.equalsIgnoreCase(serverToken.auth_value));
        }

        log.warn("Invalid AuthToken instance - wrong type or null");
        return false;
    }

    public void writeTo(DataOutput out) throws Exception {
        Bits.writeString(this.auth_value,out);
    }

    public void readFrom(DataInput in) throws Exception {
        this.auth_value = Bits.readString(in);
    }

    public int size() {
        return Util.size(this.auth_value);
    }
}
