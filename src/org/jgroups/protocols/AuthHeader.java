package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.auth.AuthToken;
import org.jgroups.conf.ClassConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
/**
 * AuthHeader is a holder object for the token that is passed from the joiner to the coordinator
 * @author Chris Mills
 * @author Bela Ban
 */
public class AuthHeader extends Header {
    protected AuthToken token=null;


    public AuthHeader() {
    }

    public AuthHeader(AuthToken token) {
        this.token=token;
    }


    public void       setToken(AuthToken token) {this.token = token;}
    public AuthToken  getToken()                {return this.token;}
    public AuthHeader token(AuthToken token)    {this.token=token; return this;}
    public AuthToken  token()                   {return this.token;}


    public void writeTo(DataOutput out) throws Exception {
        writeAuthToken(out, token);
    }

    public void readFrom(DataInput in) throws Exception {
        this.token=readAuthToken(in);
    }

    public int size() {
        return sizeOf(token);
    }

    public String toString() {
        return "token=" + token;
    }


    protected static void writeAuthToken(DataOutput out, AuthToken tok) throws Exception {
        out.writeByte(tok == null? 0 : 1);
        if(tok == null) return;
        short id=ClassConfigurator.getMagicNumber(tok.getClass());
        out.writeShort(id);
        if(id < 0) {
            String classname=tok.getClass().getName();
            out.writeUTF(classname);
        }
        tok.writeTo(out);
    }

    protected static AuthToken readAuthToken(DataInput in) throws Exception {
        if(in.readByte() == 0) return null;
        short id=in.readShort();
        Class<?> clazz;
        if(id >= 0) {
            clazz=ClassConfigurator.get(id);
        }
        else {
            String classname=in.readUTF();
            clazz=Class.forName(classname);
        }
        AuthToken retval=(AuthToken)clazz.newInstance();
        retval.readFrom(in);
        return retval;
    }

    protected static int sizeOf(AuthToken tok) {
        int retval=Global.BYTE_SIZE; // null token ?
        if(tok == null) return retval;

        retval+=Global.SHORT_SIZE;
        short id=ClassConfigurator.getMagicNumber(tok.getClass());
        if(id < 0) {
            String classname=tok.getClass().getName();
            retval+=classname.length() +2;
        }
        retval+=tok.size();
        return retval;
    }


}

