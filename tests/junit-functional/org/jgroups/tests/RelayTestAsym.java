package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.relay.RELAY3;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
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
        MySiteConfig site_cfg=new MySiteConfig(HF, HF_NET1).addForward(".*", NET1);
        a=createNode(RELAY3.class, HF, "A", 1, connect, site_cfg);
        b=createNode(RELAY3.class, HF, "B", 1, connect, site_cfg);
        c=createNode(RELAY3.class, HF, "C", 1, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b,c);

        site_cfg=new MySiteConfig(NET1, NET1_NET2, HF_NET1).addForward(NET3, NET2);
        d=createNode(RELAY3.class, NET1, "D", 1, connect, site_cfg);
        e=createNode(RELAY3.class, NET1, "E", 1, connect, site_cfg);
        f=createNode(RELAY3.class, NET1, "F", 1, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, d,e,f);

        site_cfg=new MySiteConfig(NET2, NET1_NET2, NET2_NET3).addForward(HF, NET1);
        m=createNode(RELAY3.class, NET2, "M", 1, connect, site_cfg);
        n=createNode(RELAY3.class, NET2, "N", 1, connect, site_cfg);
        o=createNode(RELAY3.class, NET2, "O", 1, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, m,n,o);

        site_cfg=new MySiteConfig(NET3, NET2_NET3).addForward(".*", NET2);
        x=createNode(RELAY3.class, NET3, "X", 1, connect, site_cfg);
        y=createNode(RELAY3.class, NET3, "Y", 1, connect, site_cfg);
        z=createNode(RELAY3.class, NET3, "Z", 1, connect, site_cfg);
        if(connect)
            Util.waitUntilAllChannelsHaveSameView(3000, 100, x,y,z);
        assert allChannels().stream()// .peek(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()))
          .map(JChannel::getView).allMatch(v -> v.size() == 3);
    }

    protected List<JChannel> allChannels() {return Arrays.asList(a,b,c,d,e,f,m,n,o,x,y,z);}
}
