package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.jgroups.util.Util.shutdown;

/**
 * Tests use cases for {@link ASYM_ENCRYPT} described in https://issues.jboss.org/browse/JGRP-2021.
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups={Global.FUNCTIONAL,Global.ENCRYPT},singleThreaded=true)
public class ASYM_ENCRYPT_Test extends EncryptTest {
    protected static final String KEYSTORE="keystore.jks";
    protected static final String KEYSTORE_PWD="password";
    protected static final String ROGUE_KEYSTORE="rogue.jks";

    protected static final Consumer<List<Protocol>> CHANGE_KEYSTORE=
      prots -> prots.stream().filter(p -> p instanceof SSL_KEY_EXCHANGE)
        .forEach(ssl -> {
            SSL_KEY_EXCHANGE ke=(SSL_KEY_EXCHANGE)ssl;
            ke.setKeystoreName(ROGUE_KEYSTORE).setKeystorePassword(KEYSTORE_PWD)
              .setPortRange(5).setSocketTimeout(300);
        });


    @BeforeMethod protected void init() throws Exception {
        super.init();
    }

    protected boolean useExternalKeyExchange() {return false;}

    @AfterMethod protected void destroy() {
        super.destroy();
    }

    /** For some obscure TestNG reasons, this method is needed. Remove it and all tests are executed in separate threads,
     * which makes the testsuite fail!!! */
    public void dummy() {}



    /** Verifies that a non-member (non-coord) cannot send a JOIN-RSP to a member */
    public void nonMemberInjectingJoinResponse() throws Exception {
        Util.close(rogue);
        rogue=create("rogue", CHANGE_KEYSTORE);
        ProtocolStack stack=rogue.getProtocolStack();
        GMS gms=stack.findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1);
        DISCARD discard=new DISCARD().discardAll(true);
        stack.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        rogue.connect(cluster_name);
        assert rogue.getView().size() == 1;
        discard.discardAll(false);
        stack.removeProtocol(NAKACK2.class, UNICAST3.class);

        View rogue_view=View.create(a.getAddress(), a.getView().getViewId().getId() +5,
                                    a.getAddress(),b.getAddress(),c.getAddress(),rogue.getAddress());
        JoinRsp join_rsp=new JoinRsp(rogue_view, null);
        GMS.GmsHeader gms_hdr=new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP);
        Message rogue_join_rsp=new BytesMessage(b.getAddress(), rogue.getAddress()).putHeader(GMS_ID, gms_hdr)
          .setArray(GMS.marshal(join_rsp)).setFlag(Message.Flag.NO_RELIABILITY); // bypasses NAKACK2 / UNICAST3
        rogue.down(rogue_join_rsp);
        for(int i=0; i < 10; i++) {
            if(b.getView().size() > 3)
                break;
            Util.sleep(500);
        }
        assert b.getView().size() == 3 : String.format("B's view is %s, but should be {A,B,C}", b.getView());
    }




    public void mergeViewInjectionByNonMember() throws Exception {
        Util.close(rogue);
        rogue=create("rogue", null);
        GMS gms=rogue.getProtocolStack().findProtocol(GMS.class);
        gms.setMaxJoinAttempts(1).setJoinTimeout(1000).setLeaveTimeout(1000);

        // make the rogue member become a singletone cluster
        rogue.getProtocolStack().insertProtocol(new DISCARD().discardAll(true), ProtocolStack.Position.ABOVE, TP.class);
        rogue.connect(cluster_name);
        rogue.getProtocolStack().removeProtocol(DISCARD.class);

        MergeView merge_view=new MergeView(a.getAddress(), a.getView().getViewId().getId()+5,
                                           Arrays.asList(a.getAddress(), b.getAddress(), c.getAddress(), rogue.getAddress()), null);
        GMS.GmsHeader hdr=new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW, a.getAddress());
        Message merge_view_msg=new BytesMessage(null, marshalView(merge_view)).putHeader(GMS_ID, hdr)
          .setFlag(Message.Flag.NO_RELIABILITY);
        System.out.printf("** %s: trying to install MergeView %s in all members\n", rogue.getAddress(), merge_view);
        rogue.down(merge_view_msg);

        // check if A, B or C installed the MergeView sent by rogue:
        for(int i=0; i < 10; i++) {
            boolean rogue_views_installed=Stream.of(a,b,c).anyMatch(ch -> ch.getView().containsMember(rogue.getAddress()));
            if(rogue_views_installed)
                break;
            Util.sleep(500);
        }
        Stream.of(a,b,c).forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
        assert Stream.of(a, b, c).noneMatch(ch -> ch.getView().containsMember(rogue.getAddress()));
    }


    /** Tests that when {ABC} -> {AB}, neither A nor B can receive a message from non-member C */
    public void testMessagesByLeftMember() throws Exception {
        View view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(),b.getAddress());

        // Enable changing of the group key when a member leaves, so A and B drop C's messages after it was excluded
        forAll(ASYM_ENCRYPT.class, asym -> asym.setChangeKeyOnLeave(true), a,b);

        GMS gms_a=a.getProtocolStack().findProtocol(GMS.class);
        gms_a.castViewChangeAndSendJoinRsps(view, null, Collections.singletonList(b.getAddress()), null, null);

        printSymVersion(a,b,c);

        Util.sleep(1000); // give members time to handle the new view
        c.send(null, "hello from left member C!");
        c.send(a.getAddress(), "hello from C");
        c.send(b.getAddress(), "hello from C");
        for(int i=0; i < 10; i++) {
            if(ra.size() > 0 || rb.size() > 0)
                break;
            Util.sleep(500);
        }
        assert ra.size() == 0 && rb.size() == 0: String.format("A and/or B: received msgs from non-member C: %s / %s",
                                                               print(ra.list()), print(rb.list()));
    }

    /** Tests that a left member C cannot decrypt messages from the cluster */
    public void testEavesdroppingByLeftMember() throws Exception {
        printSymVersion(a,b,c);
        View view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(),b.getAddress());
        GMS gms_a=a.getProtocolStack().findProtocol(GMS.class);

        // Enable changing of the group key when a member leaves, so A and B drop C's messages after it was excluded
        forAll(ASYM_ENCRYPT.class, asym -> asym.setChangeKeyOnLeave(true), a,b);

        gms_a.castViewChangeAndSendJoinRsps(view, null, Collections.singletonList(b.getAddress()), null, null);

        printSymVersion(a,b,c);
        c.getProtocolStack().removeProtocol(NAKACK2.class); // to prevent A and B from discarding C as non-member

        Util.sleep(2000); // give members time to handle the new view
        a.send(null, "hello from A");
        b.send(null, "hello from B");

        for(int i=0; i < 10; i++) {
            if(rc.size() > 0 && rc.list().stream().anyMatch(m -> m.getLength() > 0))
                break;
            Util.sleep(1000);
        }
        assert rc.size() == 0 : String.format("C: received msgs from cluster: %s", print(rc.list()));
    }

    /**
     * Tests {A,B,C} with A crashing. B installs a new view with a freshly created secret key SK. However, C won't be
     * able to decrypt the new view as it doesn't have SK.<br/>
     * https://issues.jboss.org/browse/JGRP-2203
     */
    public void testCrashOfCoord() throws Exception {
        Address crashed_coord=a.getAddress();
        shutdown(a);

        //System.out.printf("** Crashing %s **\n", crashed_coord);
        GMS gms=b.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.SUSPECT, Collections.singletonList(crashed_coord)));

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, b,c);
        for(JChannel ch: Arrays.asList(b,c))
            System.out.printf("View for %s: %s\n", ch.getName(), ch.getView());
        for(JChannel ch: Arrays.asList(b,c)) {
            assert ch.getView().size() == 2;
            assert ch.getView().containsMember(b.address());
            assert ch.getView().containsMember(c.address());
        }
    }

    /**
     * Tests A,B,C with C leaving gracefully and ASYM_ENCRYPT.change_key_on_leave=true. A installs a new secret key,
     * which B doesn't understand. However, B fetches the secret key from A and is now able to install the new view B,C.
     * @throws Exception
     */
    public void testLeaveOfParticipant() throws Exception {
        for(JChannel ch: Arrays.asList(a,b)) {
            ASYM_ENCRYPT encr=ch.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
            encr.change_key_on_leave=true;
        }
        Util.close(c);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);
        for(JChannel ch: Arrays.asList(a,b))
            System.out.printf("View for %s: %s\n", ch.getName(), ch.getView());
        for(JChannel ch: Arrays.asList(a,b)) {
            assert ch.getView().size() == 2;
            assert ch.getView().containsMember(a.address());
            assert ch.getView().containsMember(b.address());
        }
    }

    public void testMerge() throws Exception {
        Util.close(rogue);
        d=create("D", null);
        d.connect(getClass().getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b,c,d);

        GMS gms_a=a.getProtocolStack().findProtocol(GMS.class), gms_c=c.getProtocolStack().findProtocol(GMS.class);
        printSymVersion(a,b,c,d);
        Util.sleep(500);

        System.out.println("\n=== Injecting view {A,B} into A and B, and {C,D} into C and D ===\n");
        View a_view=View.create(a.getAddress(), a.getView().getViewId().getId()+1, a.getAddress(), b.getAddress());
        View c_view=View.create(c.getAddress(), c.getView().getViewId().getId()+1, c.getAddress(), d.getAddress());

        discardTraffic(a, c.getAddress(), d.getAddress()); // A,B discard traffic from C,D
        discardTraffic(b, c.getAddress(), d.getAddress());
        discardTraffic(c, a.getAddress(), b.getAddress()); // C,D discard traffic from A,B
        discardTraffic(d, a.getAddress(), b.getAddress());

        gms_a.castViewChangeAndSendJoinRsps(a_view, null, Arrays.asList(a.getAddress(), b.getAddress()), null, null);
        gms_c.castViewChangeAndSendJoinRsps(c_view, null, Arrays.asList(c.getAddress(), d.getAddress()), null, null);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, c,d);
        printSymVersion(a,b,c,d);

        Stream.of(a,b,c,d).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        Address leader=determineLeader(a,c);
        JChannel leader_channel=Stream.of(a,b,c,d).filter(ch -> leader.equals(ch.getAddress())).findFirst()
          .orElse(null);
        GMS gms=leader_channel.getProtocolStack().findProtocol(GMS.class);
        System.out.printf("\n=== Injecting merge event into leader %s ===\n", leader);

        Map<Address,View> merge_views=new HashMap<>();
        Stream.of(a,b,c,d).forEach(ch -> merge_views.put(ch.getAddress(), ch.getView()));
        gms.up(new Event(Event.MERGE, merge_views));

        Util.waitUntilAllChannelsHaveSameView(1000000, 1000, a,b,c,d); // todo: reduce timeout
        printSymVersion(a,b,c,d);
    }


    public void testObjectMessage() throws Exception {
        Person p=new Person("Bela Ban", 54, Util.generateArray(1200));
        Message msg=new ObjectMessage(b.getAddress(), p);
        a.send(msg);
        Util.waitUntil(5000, 500, () -> rb.size() == 1);
        Message m=rb.list().get(0);
        assert m.getClass().equals(msg.getClass()) : String.format("expected %s, but got %s", msg.getClass(), m.getClass());
        Person p2=m.getObject();
        assert p2.name.equals(p.name) && p2.age == p.age;
        Util.verifyArray(p2.buf);
    }


    @Override protected JChannel create(String name, Consumer<List<Protocol>> c) throws Exception {
        List<Protocol> protocols=new ArrayList<>(Arrays.asList(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          // omit MERGE3 from the stack -- nodes are leaving gracefully
          new SSL_KEY_EXCHANGE().setKeystoreName(KEYSTORE).setKeystorePassword(KEYSTORE_PWD).setPortRange(10),
          new ASYM_ENCRYPT().setUseExternalKeyExchange(useExternalKeyExchange())
            .symKeylength(128).symAlgorithm(symAlgorithm()).symIvLength(symIvLength()).asymKeylength(512).asymAlgorithm("RSA"),
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(2000)));
        if(c != null)
            c.accept(protocols);
        return new JChannel(protocols).name(name);
    }

    protected static void printSymVersion(JChannel... channels) {
        for(JChannel ch: channels) {
            ASYM_ENCRYPT encr=ch.getProtocolStack().findProtocol(ASYM_ENCRYPT.class);
            byte[] sym_version=encr.symVersion();
            System.out.printf("%s: %s [%s]\n", ch.getAddress(), ch.getView(), Util.byteArrayToHexString(sym_version));
        }
    }


    protected static ByteArray marshalView(final View view) throws Exception {
        final ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(Global.SHORT_SIZE + view.serializedSize());
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

    protected static Address determineLeader(JChannel... channels) {
        Membership membership=new Membership();
        for(JChannel ch: channels)
            membership.add(ch.getAddress());
        return membership.sort().elementAt(0);
    }

    protected static void discardTraffic(JChannel ch, Address ... addrs) {
        ProtocolStack stack=ch.getProtocolStack();
        DISCARD d=new DISCARD().addIgnoredMembers(addrs);
        stack.insertProtocolInStack(d, stack.getTransport(), ProtocolStack.Position.ABOVE);
    }

    protected static void forAll(Class<? extends ASYM_ENCRYPT> cl, Consumer<? super ASYM_ENCRYPT> c, JChannel... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            ASYM_ENCRYPT asym=stack.findProtocol(cl);
            c.accept(asym);
        }
    }

    protected static class Person implements Serializable {
        private static final long serialVersionUID=8635045223414419580L;
        protected String name;
        protected int    age;
        protected byte[] buf;

        public Person(String name, int age, byte[] buf) {
            this.name=name;
            this.age=age;
            this.buf=buf;
        }

        public String toString() {
            return String.format("name=%s age=%d bytes=%d", name, age, buf != null? buf.length : 0);
        }
    }


}
