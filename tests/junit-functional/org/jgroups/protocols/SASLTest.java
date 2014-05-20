package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.security.auth.callback.*;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.io.IOException;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SASLTest {
    private static final String REALM = "MyRealm";
    private JChannel a;
    private JChannel b;

    private static JChannel createChannel(String channelName,String mech,String username) throws Exception {
        SASL sasl = new SASL();
        sasl.setMech(mech);
        sasl.setClientCallbackHandler(new MyCallbackHandler(username));
        sasl.setServerCallbackHandler(new MyCallbackHandler(username));
        sasl.setTimeout(5000);
        sasl.sasl_props.put("com.sun.security.sasl.digest.realm", REALM);
        return new JChannel(
                new Protocol[] {
                        new SHARED_LOOPBACK(),
                        new PING(),
                        new NAKACK2(),
                        new UNICAST3(),
                        new STABLE(),
                        sasl,
                        new GMS() }
                ).name(channelName);
    }

    public void testSASLDigestMD5() throws Exception {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jack");
        a.connect("SaslTest");
        b.connect("SaslTest");
        assertTrue(b.isConnected());
    }


    @Test(expectedExceptions=SecurityException.class)
    public void testSASLDigestMD5Failure() throws Throwable {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jill");
        a.connect("SaslTest");
        try {
            b.connect("SaslTest");
        } catch (Exception e) {
            if (e.getCause() != null)
                throw e.getCause();
        }
    }

    @AfterMethod
    public void cleanup() {
        a.close();
        b.close();
    }


    public static class MyCallbackHandler implements CallbackHandler {
        final private String password;

        public MyCallbackHandler(String password) {
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for(Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback)callback;
                    nameCallback.setName("user");
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback)callback;
                    passwordCallback.setPassword(password.toCharArray());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback authorizeCallback = (AuthorizeCallback)callback;
                    authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(authorizeCallback.getAuthorizationID()));
                } else if (callback instanceof RealmCallback) {
                    RealmCallback realmCallback = (RealmCallback) callback;
                    realmCallback.setText(REALM);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }

    }
}
