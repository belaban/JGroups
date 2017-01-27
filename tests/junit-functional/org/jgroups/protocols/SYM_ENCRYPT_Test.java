package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.stream.Stream;

/**
 * Tests use cases for {@link SYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * Make sure you create the keystore before running this test (ant make-keystore).
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SYM_ENCRYPT_Test extends EncryptTest {
    protected static final String DEF_PWD="changeit";

    @BeforeMethod protected void init() throws Exception {
        super.init(getClass().getSimpleName());
    }

    @AfterMethod protected void destroy() {
        super.destroy();
    }


    /** Calling methods in superclass. Kludge because TestNG doesn't call methods in superclass correctly **/
    public void testRegularMessageReception() throws Exception {
        super.testRegularMessageReception();
    }

    public void testRegularMessageReceptionWithNullMessages() throws Exception {
        super.testRegularMessageReceptionWithNullMessages();
    }

    /** Same as above, but don't encrypt entire message, but just payload */
    public void testRegularMessageReceptionWithNullMessagesEncryptOnlyPayload() throws Exception {
        Stream.of(a,b,c).forEach(ch -> {
            Encrypt encr=ch.getProtocolStack().findProtocol(Encrypt.class);
            encr.encryptEntireMessage(false);
        });
        super.testRegularMessageReceptionWithNullMessages();
    }

    public void testRegularMessageReceptionWithEmptyMessages() throws Exception {
        super.testRegularMessageReceptionWithEmptyMessages();
    }

    public void testRegularMessageReceptionWithEmptyMessagesEncryptOnlyPayload() throws Exception {
        Stream.of(a,b,c).forEach(ch -> {
            Encrypt encr=ch.getProtocolStack().findProtocol(Encrypt.class);
            encr.encryptEntireMessage(false);
        });
        super.testRegularMessageReceptionWithEmptyMessages();
    }

    public void testChecksum() throws Exception {
        super.testChecksum();
    }

    public void testRogueMemberJoin() throws Exception {
        super.testRogueMemberJoin();
    }

    public void testMessageSendingByRogue() throws Exception {
        super.testMessageSendingByRogue();
    }

    public void testMessageSendingByRogueUsingEncryption() throws Exception {
        super.testMessageSendingByRogueUsingEncryption();
    }

    public void testMessageReceptionByRogue() throws Exception {
        super.testMessageReceptionByRogue();
    }

    public void testCapturingOfMessageByNonMemberAndResending() throws Exception {
        super.testCapturingOfMessageByNonMemberAndResending();
    }

    public void testRogueViewInstallation() throws Exception {
        super.testRogueViewInstallation();
    }



    protected JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        SYM_ENCRYPT encrypt;
        try {
            encrypt=createENCRYPT("keystore/defaultStore.keystore", DEF_PWD);
        }
        catch(Throwable t) {
            encrypt=createENCRYPT("defaultStore.keystore", DEF_PWD);
        }
        ch.getProtocolStack().insertProtocol(encrypt, ProtocolStack.Position.BELOW, NAKACK2.class);
        return ch;
    }


    // Note that setting encrypt_entire_message to true is critical here, or else some of the tests in this
    // unit test would fail!
    protected SYM_ENCRYPT createENCRYPT(String keystore_name, String store_pwd) throws Exception {
        SYM_ENCRYPT encrypt=new SYM_ENCRYPT().keystoreName(keystore_name).alias("myKey")
          .storePassword(store_pwd).encryptEntireMessage(true).signMessages(true);
        encrypt.init();
        return encrypt;
    }

}
