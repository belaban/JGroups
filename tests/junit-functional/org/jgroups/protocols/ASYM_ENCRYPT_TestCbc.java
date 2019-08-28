package org.jgroups.protocols;

import org.testng.annotations.AfterMethod;

/**
 * Exercise ASYM_ENCRYPT_Test with CBC mode cipher.
 */
public class ASYM_ENCRYPT_TestCbc extends ASYM_ENCRYPT_Test {

    @Override protected String symAlgorithm() { return "AES/CBC/PKCS5Padding"; }
    @Override protected int symIvLength() { return 16; }

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy2() {}

}
