package org.jgroups.protocols;

import org.jgroups.Global;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for AZURE_PING protocol.
 *
 * @author Radoslav Husar
 * @version Jun 2015
 */
@Test(groups = {Global.STACK_INDEPENDENT})
public class AZURE_PINGTest {

    private AZURE_PING azure;

    @BeforeMethod
    void setUp() {
        azure = new AZURE_PING();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidationMissingConfiguration() {
        azure.validateConfiguration();
    }

    @Test
    public void testValidationAllConfigured() {
        azure.storage_account_name = "myaccount";
        azure.storage_access_key = "1wsRK265DNm8v7LxT6txU2qZ_3DsBnbv";
        azure.container = "mycontainer";
        azure.validateConfiguration();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testValidationWrongContainerName() {
        azure.storage_account_name = "myaccount";
        azure.storage_access_key = "1wsRK265DNm8v7LxT6txU2qZ_3DsBnbv";
        azure.container = "MY_CONTAINER";
        azure.validateConfiguration();
    }

}
