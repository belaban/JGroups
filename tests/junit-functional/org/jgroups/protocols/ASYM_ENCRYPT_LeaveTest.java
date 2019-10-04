package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.tests.BaseLeaveTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests graceful leaving of the coordinator and second-in-line in a 10 node cluster with ASYM_ENCRYPT configured.
 * <br/>
 * Reproducer for https://issues.jboss.org/browse/JGRP-2297
 * @author Bela Ban
 * @since  4.0.12
 */
@Test(groups={Global.FUNCTIONAL,Global.ENCRYPT},singleThreaded=true)
public class ASYM_ENCRYPT_LeaveTest extends BaseLeaveTest {
    protected static final String      KEYSTORE="my-keystore.jks";
    protected static final String      KEYSTORE_PWD="password";

    protected boolean useExternalKeyExchange() {return false;}
    protected String symAlgorithm() { return "AES"; }
    protected int symIvLength() { return 0; }

    @AfterMethod protected void destroy() {
        super.destroy();
    }

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy() {}


    /** Creates a channel with a config similar to ./conf/asym-ssl.xml */
    protected JChannel create(String name) throws Exception {
        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          // omit MERGE3 from the stack -- nodes are leaving gracefully
          new SSL_KEY_EXCHANGE().setKeystoreName(KEYSTORE).setKeystorePassword(KEYSTORE_PWD)
            .setPortRange(30).setPort(2257),
          new ASYM_ENCRYPT().setUseExternalKeyExchange(useExternalKeyExchange())
            .symKeylength(128).symAlgorithm(symAlgorithm()).symIvLength(symIvLength()).asymKeylength(512).asymAlgorithm("RSA"),
          new NAKACK2().setUseMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(2000))
          .name(name);
    }

}
