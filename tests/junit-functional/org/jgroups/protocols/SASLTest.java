package org.jgroups.protocols;

import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Membership;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SASLTest {
    private static final String REALM = "MyRealm";
    private JChannel a;
    private JChannel b;

    private static JChannel createChannel(String channelName, String mech, String username) throws Exception {
        SASL sasl = new SASL();
        sasl.setMech(mech);
        sasl.setClientCallbackHandler(new MyCallbackHandler(username));
        sasl.setServerCallbackHandler(new MyCallbackHandler(username));
        sasl.setTimeout(5000);
        sasl.sasl_props.put("com.sun.security.sasl.digest.realm", REALM);
        sasl.setLevel("trace");
        GMS gms = new GMS();
        gms.setJoinTimeout(3000);
        return new JChannel(new Protocol[] { new SHARED_LOOPBACK(), new PING(), new MERGE3(), new NAKACK2(),
                new UNICAST3(), new STABLE(), sasl, gms }).name(channelName);
    }

    public void testSASLDigestMD5() throws Exception {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jack");
        a.connect("SaslTest");
        b.connect("SaslTest");
        assertTrue(b.isConnected());
    }

    @Test(expectedExceptions = SecurityException.class)
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

    public void testSASLDigestMD5Merge() throws Exception {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jack");
        a.connect("SaslTest");
        b.connect("SaslTest");
        assertTrue(b.isConnected());
        print(a, b);
        createPartitions(a, b);
        print(a, b);
        assertTrue(checkViewSize(1, a, b));
        dropDiscard(a, b);
        mergePartitions(a, b);
        for(int i = 0; i < 10 && !checkViewSize(2, a, b); i++) {
            Util.sleep(500);
        }
        assertTrue(viewContains(a.getView(), a, b));
        assertTrue(viewContains(b.getView(), a, b));
    }

    private boolean viewContains(View view, JChannel... channels) {
        boolean b = true;
        for (JChannel ch : channels) {
            b = b && view.containsMember(ch.getAddress());
        }
        return b;
    }

    private void dropDiscard(JChannel... channels) {
        for (JChannel ch : channels) {
            ch.getProtocolStack().removeProtocol(DISCARD.class);
        }
    }

    private boolean checkViewSize(int expectedSize, JChannel... channels) {
        boolean b = true;
        for (JChannel ch : channels) {
            b = b && ch.getView().size() == expectedSize;
        }
        return b;
    }

    @AfterMethod
    public void cleanup() {
        a.close();
        b.close();
    }

    private static void createPartitions(JChannel... channels) throws Exception {
        for (JChannel ch : channels) {
            DISCARD discard = new DISCARD();
            discard.setDiscardAll(true);
            ch.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        }

        for (JChannel ch : channels) {
            View view = View.create(ch.getAddress(), 10, ch.getAddress());
            GMS gms = (GMS) ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    private static void mergePartitions(JChannel... channels) throws Exception {
        Membership membership = new Membership();
        for (JChannel ch : channels) {
            membership.add(ch.getAddress());
        }
        membership.sort();
        Address leaderAddress = membership.elementAt(0);
        JChannel leader = findChannelByAddress(leaderAddress, channels);
        GMS gms = (GMS) leader.getProtocolStack().findProtocol(GMS.class);
        gms.setLevel("trace");
        Map<Address, View> views =new HashMap<>();
        for (JChannel ch : channels) {
            views.put(ch.getAddress(), ch.getView());
        }
        gms.up(new Event(Event.MERGE, views));
    }

    private static JChannel findChannelByAddress(Address address, JChannel... channels) {
        for (JChannel ch : channels) {
            if (ch.getAddress().equals(address)) {
                return ch;
            }
        }
        return null;
    }

    private static void print(JChannel... channels) {
        for (JChannel ch : channels) {
            System.out.println(ch.getName() + ": " + ch.getView());
        }
    }

    public static class MyCallbackHandler implements CallbackHandler {
        final private String password;

        public MyCallbackHandler(String password) {
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback) callback;
                    nameCallback.setName("user");
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback) callback;
                    passwordCallback.setPassword(password.toCharArray());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
                    authorizeCallback.setAuthorized(
                            authorizeCallback.getAuthenticationID().equals(authorizeCallback.getAuthorizationID()));
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
