package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.auth.MD5Token;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.DeltaView;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Buffer;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.util.Arrays;

/**
 * Tests use cases for {@link ASYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ASYM_ENCRYPT_Test extends EncryptTest {
    protected static final short  ASYM_ENCRYPT_ID;

    static {
        ASYM_ENCRYPT_ID=ClassConfigurator.getProtocolId(ASYM_ENCRYPT.class);
    }


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

    public void testRegularMessageReceptionWithEmptyMessages() throws Exception {
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



    /**
     * A non-member sends a {@link EncryptHeader#SECRET_KEY_REQ} request to the key server. Asserts that the rogue member
     * doesn't get the secret key. If it did, it would be able to decrypt all messages from cluster members!
     */
    public void nonMemberGetsSecretKeyFromKeyServer() throws Exception {
        Util.close(rogue);

        rogue=new JChannel(Util.getTestStack()).name("rogue");
        DISCARD discard=new DISCARD().setDiscardAll(true);
        rogue.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        CustomENCRYPT encrypt=new CustomENCRYPT();
        encrypt.init();

        rogue.getProtocolStack().insertProtocol(encrypt, ProtocolStack.BELOW, NAKACK2.class);
        rogue.connect(cluster_name); // creates a singleton cluster

        assert rogue.getView().size() == 1;
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        View rogue_view=new View(a.getAddress(), a.getView().getViewId().getId(),
                                 Arrays.asList(a.getAddress(),b.getAddress(),c.getAddress(),rogue.getAddress()));
        gms.installView(rogue_view);


        // now fabricate a KEY_REQUEST message and send it to the key server (A)
        Message newMsg=new Message(a.getAddress(), rogue.getAddress(), encrypt.keyPair().getPublic().getEncoded())
          .putHeader(encrypt.getId(),new EncryptHeader(EncryptHeader.SECRET_KEY_REQ, encrypt.symVersion()));

        discard.setDiscardAll(false);
        System.out.printf("-- sending KEY_REQUEST to key server %s\n", a.getAddress());
        encrypt.getDownProtocol().down(new Event(Event.MSG, newMsg));
        for(int i=0; i < 10; i++) {
            SecretKey secret_key=encrypt.key;
            if(secret_key != null)
                break;
            Util.sleep(500);
        }

        discard.setDiscardAll(true);
        gms.installView(View.create(rogue.getAddress(), 20, rogue.getAddress()));
        System.out.printf("-- secret key is %s (should be null)\n", encrypt.key);
        assert encrypt.key == null : String.format("should not have received secret key %s", encrypt.key);
    }



    /** Verifies that a non-member (non-coord) cannot send a JOIN-RSP to a member */
    public void nonMemberInjectingJoinResponse() throws Exception {
        Util.close(rogue);
        rogue=create("rogue");
        ProtocolStack stack=rogue.getProtocolStack();
        AUTH auth=(AUTH)stack.findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1);
        DISCARD discard=new DISCARD().setDiscardAll(true);
        stack.insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        rogue.connect(cluster_name);
        assert rogue.getView().size() == 1;
        discard.setDiscardAll(false);
        stack.removeProtocol(NAKACK2.class, UNICAST3.class);

        View rogue_view=View.create(a.getAddress(), a.getView().getViewId().getId() +5,
                                    a.getAddress(),b.getAddress(),c.getAddress(),rogue.getAddress());
        JoinRsp join_rsp=new JoinRsp(rogue_view, null);
        GMS.GmsHeader gms_hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP);
        Message rogue_join_rsp=new Message(b.getAddress(), rogue.getAddress()).putHeader(GMS_ID, gms_hdr)
          .setBuffer(GMS.marshal(join_rsp)).setFlag(Message.Flag.NO_RELIABILITY); // bypasses NAKACK2 / UNICAST3
        rogue.down(new Event(Event.MSG, rogue_join_rsp));
        for(int i=0; i < 10; i++) {
            if(b.getView().size() > 3)
                break;
            Util.sleep(500);
        }
        assert b.getView().size() == 3 : String.format("B's view is %s, but should be {A,B,C}", b.getView());
    }



    /** The rogue node has an incorrect {@link AUTH} config (secret) and can thus not join */
    public void rogueMemberCannotJoinDueToAuthRejection() throws Exception {
        Util.close(rogue);
        rogue=create("rogue");
        AUTH auth=(AUTH)rogue.getProtocolStack().findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(2);
        rogue.connect(cluster_name);
        System.out.printf("Rogue's view is %s\n", rogue.getView());
        assert rogue.getView().size() == 1 : String.format("rogue should have a singleton view of itself, but doesn't: %s", rogue.getView());
    }


    public void mergeViewInjectionByNonMember() throws Exception {
        Util.close(rogue);
        rogue=create("rogue");
        AUTH auth=(AUTH)rogue.getProtocolStack().findProtocol(AUTH.class);
        auth.setAuthToken(new MD5Token("unknown_pwd"));
        GMS gms=(GMS)rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1);
        rogue.connect(cluster_name);

        MergeView merge_view=new MergeView(a.getAddress(), a.getView().getViewId().getId()+5,
                                           Arrays.asList(a.getAddress(), b.getAddress(), c.getAddress(), rogue.getAddress()), null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW, a.getAddress());
        Message merge_view_msg=new Message(null, marshalView(merge_view)).putHeader(GMS_ID, hdr)
          .setFlag(Message.Flag.NO_RELIABILITY);
        System.out.printf("** %s: trying to install MergeView %s in all members\n", rogue.getAddress(), merge_view);
        rogue.down(new Event(Event.MSG, merge_view_msg));

        // check if A, B or C installed the MergeView sent by rogue:
        for(int i=0; i < 10; i++) {
            boolean rogue_views_installed=false;

            for(JChannel ch: Arrays.asList(a,b,c))
                if(ch.getView().containsMember(rogue.getAddress()))
                    rogue_views_installed=true;
            if(rogue_views_installed)
                break;
            Util.sleep(500);
        }
        for(JChannel ch: Arrays.asList(a,b,c))
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
        for(JChannel ch: Arrays.asList(a,b,c))
            assert !ch.getView().containsMember(rogue.getAddress());
    }


    /** Tests that when {ABC} -> {AB}, neither A nor B can receive a message from non-member C */
    public void testMessagesByLeftMember() throws Exception {
        View view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(),b.getAddress());
        for(JChannel ch: Arrays.asList(a,b)) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        };
        for(JChannel ch: Arrays.asList(a,b))
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
        System.out.printf("%s: %s\n", c.getAddress(), c.getView());

        c.getProtocolStack().removeProtocol(NAKACK2.class); // to prevent A and B from discarding C as non-member

        Util.sleep(1000); // give members time to handle the new view
        c.send(null, "hello world from left member C!");
        for(int i=0; i < 10; i++) {
            if(ra.size() > 0 || rb.size() > 0)
                break;
            Util.sleep(500);
        }
        assert ra.size() == 0 : String.format("A: received msgs from non-member C: %s", print(ra.list()));
        assert rb.size() == 0 : String.format("B: received msgs from non-member C: %s", print(rb.list()));
    }

    /** Tests that a left member C cannot decrypt messages from the cluster */
    public void testEavesdroppingByLeftMember() throws Exception {
        printSymVersion(a,b,c);
        for(JChannel ch: Arrays.asList(a,b)) {
            ASYM_ENCRYPT encr=(ASYM_ENCRYPT)ch.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
            encr.minTimeBetweenKeyRequests(500);
        }
        View view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(),b.getAddress());
         for(JChannel ch: Arrays.asList(a,b)) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        };
        for(JChannel ch: Arrays.asList(a,b))
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
        System.out.printf("%s: %s\n", c.getAddress(), c.getView());
        c.getProtocolStack().removeProtocol(NAKACK2.class); // to prevent A and B from discarding C as non-member

        Util.sleep(2000); // give members time to handle the new view

        printSymVersion(a,b,c);
        a.send(null, "hello from A");
        b.send(null, "hello from B");

        for(int i=0; i < 10; i++) {
            if(rc.size() > 0)
                break;
            Util.sleep(500);
        }
        assert rc.size() == 0 : String.format("C: received msgs from cluster: %s", print(rc.list()));
    }


    protected JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        ProtocolStack stack=ch.getProtocolStack();
        EncryptBase encrypt=createENCRYPT();
        stack.insertProtocol(encrypt, ProtocolStack.BELOW, NAKACK2.class);
        AUTH auth=new AUTH();
        auth.setAuthCoord(true);
        auth.setAuthToken(new MD5Token("mysecret")); // .setAuthCoord(false);
        stack.insertProtocol(auth, ProtocolStack.BELOW, GMS.class);
        stack.findProtocol(GMS.class).setValue("join_timeout", 2000); // .setValue("view_ack_collection_timeout", 10);
        return ch;
    }

    protected void printSymVersion(JChannel ... channels) {
        for(JChannel ch: channels) {
            ASYM_ENCRYPT encr=(ASYM_ENCRYPT)ch.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
            byte[] sym_version=encr.symVersion();
            System.out.printf("sym-version %s: %s\n", ch.getAddress(), Util.byteArrayToHexString(sym_version));
        }
    }


    // Note that setting encrypt_entire_message to true is critical here, or else some of the tests in this
    // unit test would fail!
    protected ASYM_ENCRYPT createENCRYPT() throws Exception {
        ASYM_ENCRYPT encrypt=new ASYM_ENCRYPT().encryptEntireMessage(true).signMessages(true);
        encrypt.init();
        return encrypt;
    }


    protected static Buffer marshalView(final View view) throws Exception {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(512);
        out.writeShort(determineFlags(view));
        view.writeTo(out);
        return out.getBuffer();
    }

    protected static short determineFlags(final View view) {
        short retval=0;
        if(view != null) {
            retval|=GMS.VIEW_PRESENT;
            if(view instanceof MergeView)
                retval|=GMS.MERGE_VIEW;
            else if(view instanceof DeltaView)
                retval|=GMS.DELTA_VIEW;
        }
        return retval;
    }


    protected static class CustomENCRYPT extends ASYM_ENCRYPT {
        protected SecretKey key;

        public CustomENCRYPT() {
            this.id=ASYM_ENCRYPT_ID;
        }

        protected Object handleUpEvent(Message msg, EncryptHeader hdr) {
            if(hdr.type() == EncryptHeader.SECRET_KEY_RSP) {
                try {
                    key=decodeKey(msg.getBuffer());
                    System.out.printf("received secret key %s !\n", key);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            return super.handleUpEvent(msg, hdr);
        }
    }

}
