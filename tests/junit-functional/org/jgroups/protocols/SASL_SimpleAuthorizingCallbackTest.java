package org.jgroups.protocols;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.auth.sasl.SimpleAuthorizingCallbackHandler;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.AbstractProtocol;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SASL_SimpleAuthorizingCallbackTest {
    private static final String REALM = "MyRealm";
    private JChannel a;
    private JChannel b;
    File credentialsFile;
    File rolesFile;

    @BeforeClass
    public void initialize() throws Exception {
        Properties credentials = new Properties();
        credentials.put("jack", "brokehiscrown");
        credentials.put("jill", "cametumblingafter");
        credentials.put("jane", "whatsyourname");
        credentialsFile = File.createTempFile("sasl_credentials", ".properties");
        credentials.store(new FileOutputStream(credentialsFile), null);

        Properties roles = new Properties();
        roles.put("jack", "mycluster");
        roles.put("jill", "mycluster");
        roles.put("jane", "othercluster");
        rolesFile = File.createTempFile("sasl_roles", ".properties");
        roles.store(new FileOutputStream(rolesFile), null);
    }

    private JChannel createChannel(String channelName, String mech, String principal) throws Exception {
        Properties properties = new Properties();
        properties.put("sasl.local.principal", principal);
        properties.put("sasl.credentials.properties", credentialsFile.getAbsolutePath());
        properties.put("sasl.role", "mycluster");
        properties.put("sasl.roles.properties", rolesFile.getAbsolutePath());
        properties.put("sasl.realm", REALM);
        SASL sasl = new SASL();
        sasl.setMech(mech);
        sasl.setClientCallbackHandler(new SimpleAuthorizingCallbackHandler(properties));
        sasl.setServerCallbackHandler(new SimpleAuthorizingCallbackHandler(properties));
        sasl.setTimeout(5000);
        sasl.sasl_props.put("com.sun.security.sasl.digest.realm", REALM);
        return new JChannel(new AbstractProtocol[] { new SHARED_LOOPBACK(), new PING(), new NAKACK2(), new UNICAST3(),
                new STABLE(), sasl, new GMS() }).name(channelName);
    }

    public void testSASLDigestMD5() throws Exception {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jill");
        a.connect("SaslTest");
        b.connect("SaslTest");
        assertTrue(b.isConnected());
    }

    @Test(expectedExceptions = SecurityException.class)
    public void testSASLDigestMD5Failure() throws Throwable {
        a = createChannel("A", "DIGEST-MD5", "jack");
        b = createChannel("B", "DIGEST-MD5", "jane");
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
}
