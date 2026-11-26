package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DROP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.relay.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests asymmetric networks. The sites are setup as follows:
 * <pre>
 *    hf <--> net1 <--> net2 <--> net3
 * </pre>
 * If a member from NET3 wants to send a message to a member in HF, it needs to forward the message via NET2, which
 * in turn needs to forward it via NET1 to HF
 * @author Bela Ban
 * @since  5.2.18
 */
@Test(groups={Global.FUNCTIONAL,Global.RELAY},singleThreaded=true)
public class RelayTestAsym extends RelayTests {
    protected JChannel a,b,c; // hf
    protected JChannel d,e,f; // net1
    protected JChannel m,n,o; // net2
    protected JChannel x,y,z; // net3

    protected static final String HF="hf", NET1="net1", NET2="net2", NET3="net3";
    protected static final String HF_NET1="hf-net1", NET1_NET2="net1-net2", NET2_NET3="net2-net3";

    @AfterMethod protected void destroy() {
        Util.closeReverse(a,b,c,d,e,f,m,n,o,x,y,z);
    }

    public void testCorrectSetup() throws Exception {
        setup(true);
        waitForBridgeView(2, 3000, 100, HF_NET1, a,d);
        waitForBridgeView(2, 3000, 100, NET1_NET2, d, m);
        waitForBridgeView(2, 3000, 100, NET2_NET3, m, x);
    }

    /** Every member sends a multicast and a unicast (to everybody else); verify that everybody received all messages */
    public void testMessageSending() throws Exception {
        setup(true);
        allChannels().forEach(ch -> ch.setReceiver(new MyReceiver<Message>().rawMsgs(true)));

        // multicasts:
        allChannels().forEach(ch -> send(ch, null, String.format("from %s", ch.address())));
        assertNumMessages(12, allChannels(), true); // reset receivers

        // unicasts:
        Collection<Address> all_addrs=allChannels().stream().map(JChannel::getAddress).collect(Collectors.toSet());
        allChannels().forEach(ch -> {
            all_addrs.forEach(target -> send(ch, target, String.format("from %s", ch.address())));
        });
        assertNumMessages(12, allChannels(), true);
    }

    public void testTopology() throws Exception {
        setup(true);
        Util.waitUntilTrue(3000, 100, () -> assertTopo(allChannels()));
        for(JChannel ch: allChannels()) {
            RELAY3 r=ch.getProtocolStack().findProtocol(RELAY3.class);
            Map<String,View> cache=r.topo().cache();
            System.out.printf("%s", printTopo(List.of(ch)));
            assert cache.size() == 4 : printTopo(List.of(ch)); // 4 sites - HF, NET1-3
            assert cache.values().stream().allMatch(v -> v.size() == 3) : printTopo(List.of(ch));
        }
    }

    /** Tests sending mcasts from HF and NET3 when NET1 is down: messages should not be received across the broken link */
    public void testMessageSendingWithNet1Down() throws Exception {
        setup(true);
        // take NET1 down:
        Util.close(d,e,f);
        allChannels().stream().filter(ch -> !ch.isClosed())
          .forEach(ch -> ch.setReceiver(new MyReceiver<Message>().rawMsgs(true)));
        Stream.of(a,b,c,x,y,z).forEach(ch -> send(ch, null, String.format("from %s", ch.address())));
        // we only receive 3 messages (from own site)
        assertNumMessages(3, a,b,c,x,y,z);
    }

    /** Sends unicast messages from C:hf -> Z:net3 */
    public void testSendFromHF_To_NET3() throws Exception {
        testSendFromHF_ToDest(() -> z.address(), 1, () -> new JChannel[]{z});
    }

    public void testSendFromHF_To_NET3_2_Sitemasters() throws Exception {
        testSendFromHF_ToDest(() -> z.address(), 2, () -> new JChannel[]{z});
    }

    /** Sends messages from from C:hf -> SiteMaster("net3") */
    public void testSendFromHF_To_SM_NET3() throws Exception {
        testSendFromHF_ToDest(() -> new SiteMaster(NET3), 1, () -> new JChannel[]{x});
    }

    public void testSendFromHF_To_SM_NET3_2_Sitemasters() throws Exception {
        testSendFromHF_ToDest(() -> new SiteMaster(NET3), 2, () -> new JChannel[]{x,y});
    }

    /** Sends unicast messages from C:hf to a destination */
    protected void testSendFromHF_ToDest(Supplier<Address> s, int num_sitemasters, Supplier<JChannel[]> supplier) throws Exception {
        setup(true, num_sitemasters);
        JChannel[] receiver_channels=supplier.get();
        for(JChannel ch: receiver_channels) {
            MyReceiver<String> r=new MyReceiver<String>().verbose(true).rawMsgs(false).name(ch.name());
            ch.setReceiver(r);
        }
        // send unicasts from C:hf -> SiteMaster("net3")
        Address dest=s.get();
        for(int i=1; i <= 10; i++)
            c.send(dest,"msg-" + i);

        Util.waitUntil(3000, 100, () -> numMessages(receiver_channels) == 10);
    }

    protected static int numMessages(JChannel... channels) {
        int count=0;
        for(JChannel ch: channels)
            count+=getReceiver(ch).size();
        return count;
    }

    /** Tests sending message M from A:hf to Y:net3 while net2 is down. The site-unreachable notification should
     * stop retransmission of M on A:lon */
    public void testFailoverSiteDown() throws Exception {
        _testFailoverSiteDown(() -> y.address());
    }

    /** Tests sending message M from A:hf to SiteMaster("net3") while net2 is down. The site-unreachable notification
     * should stop retransmission of M on A:lon */
    public void testFailoverSiteDown2() throws Exception {
        _testFailoverSiteDown(() -> new SiteMaster("net3"));
    }

    @Test(enabled=false)
    protected void _testFailoverSiteDown(Supplier<Address> s) throws Exception {
        setup(true);
        waitForSiteMasters(true, a, d, m, x);
        RELAY3 relay=a.getProtocolStack().findProtocol(RELAY.class);
        relay.delaySiteUnreachableEvents(2000).delaySitesDown(true);
        Address target=s.get();
        Util.closeReverse(m, n, o); // causes entire net2 site to be down
        relay.setRouteStatusListener(new DefaultRouteStatusListener(() -> a.address()).verbose(true));
        System.out.printf("-- sending message from %s to %s\n", a.address(), target);
        a.send(target, "hello"); // won't succeed
        UNICAST3 unicast=a.getProtocolStack().findProtocol(UNICAST3.class);
        // check if there is still retransmission going on in A
        retransmissionsDone(unicast, target);
        relay.setRouteStatusListener(null);
    }

    /** A:lon sends a unicast message M to Y:net3, which left the cluster before. X:net3 (the site master) should
     * send back a HOST-UNREACHABLE message to A:lon */
    public void testHostUnreachable() throws Exception {
        setup(true);
        waitForSiteMasters(true, a, d, m, x);
        RELAY relay=a.getProtocolStack().findProtocol(RELAY.class);
        Address target=y.address();
        relay.setRouteStatusListener(new DefaultRouteStatusListener(relay::addr).verbose(true));
        Util.close(y);
        Util.waitUntil(5000, 200, () -> Stream.of(x,z).allMatch(ch -> ch.getView().size() == 2));
        System.out.printf("-- sending message to (crashed) %s:\n", target);
        final UNICAST3 unicast=a.stack().findProtocol(UNICAST3.class);
        a.send(target, "hello");

        // check if there is still retransmission going on in A
        retransmissionsDone(unicast, target);
        relay.setRouteStatusListener(null);
    }

    /** A:hf sends 5 messages to SiteMaster("net3"), but drops messages 2-5. They must be retransmitted */
    public void testRetransmissionToSiteMaster() throws Exception {
        testRetransmission(() -> new SiteMaster(NET3));
    }

    /** A:hf sends 5 messages to X:net3, but drops messages 2-5. They must be retransmitted */
    public void testRetransmissionToUnicastAddress() throws Exception {
        testRetransmission(() -> x.address());
    }

    protected void testRetransmission(Supplier<Address> supplier) throws Exception {
        setup(true);
        waitForSiteMasters(true, a, d, m, x);
        Predicate<Message> drop_filter=msg -> { // drops messages [2..4]
            if(!msg.hasPayload() || msg.dest() == null)
                return false;
            Integer num=msg.getObject();
            return num >= 2 && num <= 4;
        };
        DROP drop=new DROP().addDownFilter(drop_filter);
        a.stack().insertProtocol(drop, ProtocolStack.Position.BELOW, UNICAST3.class);
        Util.close(b,c,e,f,n,o); // we don't need them
        MyReceiver<String> r=new MyReceiver<String>().name("X").verbose(true);
        x.setReceiver(r);
        Address dest=supplier.get();
        for(int i=1; i <= 5; i++)
            a.send(dest, i);
        drop.clearAllFilters();
        Util.waitUntil(2000, 200, () -> r.size() == 5);
    }

    /**
     * A:hf sends 5 messages to SiteMaster("net3") (X), but the first 4 are dropped. When X:net3 receives message #5,
     * it asks A:hf to send the first seqno, but because the sender is X:net3, the send_table of A:hf doesn't have the
     * message, as the send-table contains SiteMaster("net3") as entry
     */
    public void testSendFirstSeqno() throws Exception {
        setup(true);
        waitForSiteMasters(true, a, d, m, x);
        DROP drop=new DROP().addDownFilter(msg -> msg.dest() != null && msg.dest().isSiteMaster());
        a.stack().insertProtocol(drop, ProtocolStack.Position.BELOW, UNICAST3.class);
        Util.close(b,c,e,f,n,o); // we don't need them
        MyReceiver<String> r=new MyReceiver<String>().name("X").verbose(true);
        x.setReceiver(r);
        Address target=new SiteMaster(NET3);
        for(int i=1; i <= 4; i++)
            a.send(target, "msg-" + i);
        drop.clearAllFilters();
        a.send(target, "msg-5"); // this message should trigger a SEND-FIRST-SEQNO message from X:net3 to A:hf
        Util.waitUntil(2000, 200, () -> r.size() == 5);
    }

    /** Sends multiple unicasts from C:hf to Z:net3. Both local and remote site masters are picked on a random
     * basis (SiteMasterPicker) */
    public void testMultipleSiteMastersUnicasts() throws Exception {
        testMultipleSiteMasters(() -> z.address());
    }

    /** Sends multiple unicasts from C:hf to SiteMaster("net3"). Both local and remote site masters are picked on
     * a random basis (SiteMasterPicker) */
    public void testMultipleSiteMastersSM() throws Exception {
        testMultipleSiteMasters(() -> new SiteMaster(NET3));
    }

    protected void testMultipleSiteMasters(Supplier<Address> supplier) throws Exception {
        setup(true, 2);
        waitForSiteMasters(true, a, d, m, x);

        Address target=supplier.get();
        System.out.printf("-- sending messages from %s to %s\n", c.address(), target);
        allChannels().stream().map(ch -> (RELAY)ch.stack().findProtocol(RELAY.class))
          .forEach(r -> {
              StickySiteMasterPicker picker=(StickySiteMasterPicker)new StickySiteMasterPicker().verbose(false);
              picker.addressSupplier(r::addr);
              r.siteMasterPicker(picker);
          });
          // .forEach(r -> r.siteMasterPicker(new RandomSiteMasterPicker().verbose(false).addressSupplier(r::addr)));
            // .forEach(r -> r.siteMasterPicker(new SiteMasterPickerImpl()));
        allChannels().forEach(ch -> ch.receiver(new MyReceiver<String>().name(ch.name()).verbose(true)));
        for(int i=1; i <= 10; i++)
            c.send(target, "msg-" + i);
        if(target.isSiteMaster()) {
            Util.waitUntilTrue(3000, 100, () -> getReceiver(x).size() + getReceiver(y).size() == 10);
            Stream.of(x,y).forEach(ch -> System.out.printf("%s: %s\n", ch.address(), getReceiver(ch).list()));
            int count=getReceiver(x).size() + getReceiver(y).size();
            assert count == 10;
        }
        else {
            JChannel target_ch=allChannels().stream().filter(ch -> Objects.equals(target, ch.address()))
              .findFirst().orElseThrow();
            MyReceiver<String> receiver=(MyReceiver<String>)target_ch.getReceiver();
            Util.waitUntil(3000, 100, () -> receiver.size() == 10);
            List<String> list=receiver.list();
            for(int i=1; i <= 10; i++) {
                String s=String.format("msg-%d", i);
                assert list.get(i-1).equals(s);
            }
        }

        allChannels().forEach(ch -> ch.setReceiver(null));

    }

    protected static void send(JChannel ch, Address dest, Object payload) {
        try {
            ch.send(dest, payload);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected static boolean assertTopo(List<JChannel> channels) {
        for(JChannel ch: channels) {
            RELAY3 r=ch.getProtocolStack().findProtocol(RELAY3.class);
            Map<String,View> cache=r.topo().cache();
            if(cache.size() != 4 || !cache.values().stream().allMatch(v -> v.size() == 3))
                return false;
        }
        return true;
    }

    protected static String printTopo(List<JChannel> channels) {
        return channels.stream()
          .map(ch -> String.format("%s:\n%s\n", ch.address(), ((RELAY3)ch.getProtocolStack().findProtocol(RELAY3.class))
            .printTopology(true))).collect(Collectors.joining("\n"));
    }

    protected void setup(boolean connect) throws Exception {
        setup(connect, 1);
    }

    protected void setup(boolean connect, int num_sitemasters) throws Exception {
        MySiteConfig site_cfg=new MySiteConfig(HF, HF_NET1).addForward(".*", NET1);
        a=createNode(RELAY3.class, HF, "A", num_sitemasters, connect, site_cfg);
        b=createNode(RELAY3.class, HF, "B", num_sitemasters, connect, site_cfg);
        c=createNode(RELAY3.class, HF, "C", num_sitemasters, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b,c);

        site_cfg=new MySiteConfig(NET1, NET1_NET2, HF_NET1).addForward(NET3, NET2);
        d=createNode(RELAY3.class, NET1, "D", num_sitemasters, connect, site_cfg);
        e=createNode(RELAY3.class, NET1, "E", num_sitemasters, connect, site_cfg);
        f=createNode(RELAY3.class, NET1, "F", num_sitemasters, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, d,e,f);

        site_cfg=new MySiteConfig(NET2, NET1_NET2, NET2_NET3).addForward(HF, NET1);
        m=createNode(RELAY3.class, NET2, "M", num_sitemasters, connect, site_cfg);
        n=createNode(RELAY3.class, NET2, "N", num_sitemasters, connect, site_cfg);
        o=createNode(RELAY3.class, NET2, "O", num_sitemasters, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, m,n,o);

        site_cfg=new MySiteConfig(NET3, NET2_NET3).addForward(".*", NET2);
        x=createNode(RELAY3.class, NET3, "X", num_sitemasters, connect, site_cfg);
        y=createNode(RELAY3.class, NET3, "Y", num_sitemasters, connect, site_cfg);
        z=createNode(RELAY3.class, NET3, "Z", num_sitemasters, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, x,y,z);
        assert !connect || allChannels().stream()
          .map(JChannel::getView).allMatch(v -> v.size() == 3);
    }

    protected List<JChannel> allChannels() {return Arrays.asList(a,b,c,d,e,f,m,n,o,x,y,z);}
}
