package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.auth.AuthToken;
import org.jgroups.conf.ClassConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

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

    public Supplier<? extends Header> create() {return AuthHeader::new;}

    public void       setToken(AuthToken token) {this.token = token;}
    public AuthToken  getToken()                {return this.token;}
    public AuthHeader token(AuthToken token)    {this.token=token; return this;}
    public AuthToken  token()                   {return this.token;}
    public short getMagicId() {return 66;}

    @Override
    public void writeTo(DataOutput out) throws IOException {
        writeAuthToken(out, token);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        this.token=readAuthToken(in);
    }

    @Override
    public int serializedSize() {
        return sizeOf(token);
    }

    public String toString() {
        return "token=" + token;
    }


    protected static void writeAuthToken(DataOutput out, AuthToken tok) throws IOException {
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

    protected static AuthToken readAuthToken(DataInput in) throws IOException, ClassNotFoundException {
        if(in.readByte() == 0) return null;
        short id=in.readShort();
        AuthToken retval=null;
        if(id >= 0)
            retval=ClassConfigurator.create(id);
        else {
            String classname=in.readUTF();
            Class<?> clazz=Class.forName(classname);
            try {
                retval=(AuthToken)clazz.getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
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

