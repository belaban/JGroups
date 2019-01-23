package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Tuple;

import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for protocols implementing key exchange: a secret key to be used for encryption is exchanged between
 * 2 parties (usually the key server and a new cluster member) <em>securely</em>; ie. without the possibility of
 * man-in-the-middle attacks, compromising the key and (optional) perfect forward secrecy.<br/>
 * This protocol has to be placed somewhere below {@link ASYM_ENCRYPT}.
 * @author Bela Ban
 * @since  4.0.5
 */
public abstract class KeyExchange extends Protocol {
    protected Address local_addr;

    public List<Integer> requiredUpServices() {
        return Arrays.asList(Event.GET_SECRET_KEY, Event.SET_SECRET_KEY);
    }

    /**
     * Needs to fetch the secret key from a given destination (usually the key server). When received, the secret key
     * (and version) needs to be installed in a protocol above using {@link #setSecretKeyAbove(Tuple)}.
     * @param target The member from which to fetch the secret key
     */
    public abstract void fetchSecretKeyFrom(Address target) throws Exception;

    /** Returns the address of the server, e.g. server socket (if any) */
    public abstract Address getServerLocation();


    public Object down(Event evt) {
        switch(evt.type()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.arg();
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.arg());
                break;
        }
        return down_prot.down(evt);
    }

    protected void handleView(View view) {}

    /** Fetches the secret key from a protocol above us
     * @return The secret key and its version
     */
    protected Tuple<SecretKey,byte[]> getSecretKeyFromAbove() {
        return (Tuple<SecretKey,byte[]>)up_prot.up(new Event(Event.GET_SECRET_KEY));
    }

    /** Sets the secret key in a protocol above us
     * @param key The secret key and its version
     */
    protected void setSecretKeyAbove(Tuple<SecretKey,byte[]> key) {
        up_prot.up(new Event(Event.SET_SECRET_KEY, key));
    }

    protected <T extends Protocol> T findProtocolAbove(Class<? extends Protocol> clazz) {
        Protocol tmp=this;
        while(tmp != null) {
            Class<?> protClass=tmp.getClass();
            if(clazz.isAssignableFrom(protClass))
                return (T)tmp;
            tmp=tmp.getUpProtocol();
        }
        return null;
    }

}
