package org.jgroups.protocols;

/**
 * Tests graceful leaving of the coordinator and second-in-line in a 10 node cluster with ASYM_ENCRYPT configured.
 * <br/>
 * Reproducer for https://issues.jboss.org/browse/JGRP-2297
 * @author Bela Ban
 * @since  4.0.12
 */
public class ASYM_ENCRYPT_LeaveTestKeyExchange extends ASYM_ENCRYPT_LeaveTest {
    @Override protected boolean useExternalKeyExchange() {return true;}

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy2() {}
}
