package org.jgroups.protocols;

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


    public void testEavesdroppingByLeftMember() throws Exception {
        System.out.println("Skipping this test as the use of an external key exchange will allow left members to " +
                            "decrypt messages received by existing members");
    }

    public void testMessagesByLeftMember() throws Exception {
        System.out.println("Skipping this test as the use of an external key exchange will allow left members to " +
                             "send messages which will be delivered by existing members, as the left member " +
                             "will be able to fetch the secret group key");
    }
}
