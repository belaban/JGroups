package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.SecureRandom;

/**
 * Tests use cases for {@link SYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * Make sure you create the keystore before running this test (ant make-keystore).
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups={Global.FUNCTIONAL,Global.ENCRYPT},singleThreaded=true)
public class SYM_ENCRYPT_Test extends EncryptTest {
    protected static final String DEF_PWD="changeit";

    @BeforeMethod protected void init() throws Exception {
        super.init();
    }

    @AfterMethod protected void destroy() {
        super.destroy();
    }

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy() {}


    protected JChannel create(String name) throws Exception {
        // Verify that the SecureRandom instance can be customized
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        SYM_ENCRYPT encr;
        try {
            encr=new SYM_ENCRYPT().keystoreName("keystore/defaultStore.keystore").alias("myKey").storePassword(DEF_PWD)
              .symAlgorithm(symAlgorithm()).symIvLength(symIvLength()).secureRandom(secureRandom);
        }
        catch(Throwable t) {
            encr=new SYM_ENCRYPT().keystoreName("defaultStore.keystore").alias("myKey").storePassword(DEF_PWD)
              .symAlgorithm(symAlgorithm()).symIvLength(symIvLength()).secureRandom(secureRandom);
        }

        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          // omit MERGE3 from the stack -- nodes are leaving gracefully
          encr,
          new NAKACK2().setUseMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(2000))
          .name(name);
    }

}
