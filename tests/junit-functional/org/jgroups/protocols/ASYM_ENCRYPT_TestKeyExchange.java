package org.jgroups.protocols;

import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;

/**
 * Tests use cases for {@link ASYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * @author Bela Ban
 * @since  4.0
 */
public class ASYM_ENCRYPT_TestKeyExchange extends ASYM_ENCRYPT_Test {

    @Override protected boolean useExternalKeyExchange() {return true;}

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy() {}


    @AfterMethod
    protected void destroy() {
        super.destroy();
    }

    public void testEavesdroppingByLeftMember() throws Exception {
        System.out.println("Skipping this test as the use of an external key exchange will allow left members to " +
                            "decrypt messages received by existing members");
    }

    public void testMessagesByLeftMember() throws Exception {
        System.out.println("Skipping this test as the use of an external key exchange will allow left members to " +
                             "send messages which will be delivered by existing members, as the left member " +
                             "will be able to fetch the secret group key");
    }

    /** A rogue member should not be able to join a cluster */
    public void testRogueMemberJoin() throws Exception {
        Util.close(rogue);
        rogue=create("rogue", CHANGE_KEYSTORE);
        GMS gms=rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(2);
        try {
            rogue.connect(cluster_name);
            assert rogue.getView().size() == 1
              : String.format("rogue member should not have been able to connect: view is %s", rogue.getView());
            Util.removeFromViews(rogue.getAddress(),a,b,c,d);
        }
        catch(SecurityException ex) {
            System.out.printf("rogue member's connect() got (expected) exception: %s\n", ex);
        }
    }
}
