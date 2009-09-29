package org.jgroups.protocols;

import org.jgroups.Header;
import org.jgroups.auth.AuthToken;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
/**
 * AuthHeader is a holder object for the token that is passed from the joiner to the coordinator
 * @author Chris Mills
 */
public class AuthHeader extends Header implements Streamable{
    private AuthToken token=null;
    private static final long serialVersionUID=9084313338045023807L;

    public AuthHeader(){
    }
    /**
     * Sets the token value to that of the passed in token object
     * @param token the new authentication token
     */
    public void setToken(AuthToken token){
        this.token = token;
    }

    /**
     * Used to get the token from the AuthHeader
     * @return the token found inside the AuthHeader
     */
    public AuthToken getToken(){
        return this.token;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.token = (AuthToken)in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.token);
    }

    public void writeTo(DataOutputStream out) throws IOException {
        Util.writeAuthToken(this.token, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        this.token = Util.readAuthToken(in);
    }
    public int size(){
        //need to fix this
        return Util.sizeOf(this);
    }
}

