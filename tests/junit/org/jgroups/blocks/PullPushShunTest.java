package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id: PullPushShunTest.java,v 1.5 2008/04/08 07:19:10 belaban Exp $
 */
public class PullPushShunTest extends TestCase implements MessageListener, MembershipListener, ChannelListener {
    private JChannel channel;
    PullPushAdapter adapter;


    public static Test suite() {
        return new TestSuite(PullPushShunTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected void setUp() throws Exception {
        ;
    }

    protected void tearDown() throws Exception {
        ;
    }

    public void testShunningandReconnect() throws Exception {
        Address old_local_addr, new_local_addr;
        channel=new JChannel();
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.addChannelListener(this);
        channel.connect("PullPushTestShun");
        adapter=new PullPushAdapter(channel, this, this);
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

    public void receive(Message msg) {
        System.out.println("-- received " + msg);
    }

    public byte[] getState() {
        return new byte[0];
    }

    public void setState(byte[] state) {
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- view: " + new_view);
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }

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
}
