package org.jgroups.blocks;



import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: PullPushShunTest.java,v 1.9 2008/04/21 12:05:27 belaban Exp $
 */
@Test(groups="temp")
public class PullPushShunTest extends ChannelTestBase {
    private JChannel channel;
    private PullPushAdapter adapter;


    public void testShunningandReconnect() throws Exception {
        Address old_local_addr, new_local_addr;
        channel=createChannel(true);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.addChannelListener(new ChannelListener() {

            public void channelConnected(Channel channel) {
                System.out.println("-- channelConnected()");
            }

            public void channelDisconnected(Channel channel) {
                System.out.println("-- channelDisconnected()");
            }

            public void channelClosed(Channel channel) {
                System.out.println("-- channelClosed()");
            }

            public void channelShunned() {
                System.out.println("-- channelShunned()");
            }

            public void channelReconnected(Address addr) {
                System.out.println("-- channelReconnected(" + addr + ")");
            }
        });
        channel.connect("PullPushTestShun");
        adapter=new PullPushAdapter(channel, null, null);
        assertEquals(1, channel.getView().getMembers().size());
        old_local_addr=channel.getLocalAddress();
        assertNotNull(old_local_addr);

        Util.sleep(1000);
        System.out.println("shunning channel");
        shun();
        Util.sleep(5000);
        new_local_addr=channel.getLocalAddress();
        assertNotNull(new_local_addr);
        channel.close();
    }

    private void shun() {
        channel.up(new Event(Event.EXIT));
    }




}
