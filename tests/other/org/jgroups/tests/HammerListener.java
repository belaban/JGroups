package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
/*
 * @author Bob Stevenson - HAMMER
 * @author Ananda Bollu - FLOW_CONTROL
 */
public class HammerListener implements ChannelListener, MembershipListener {
    private static JChannel channel = null;
    private static RpcDispatcher disp;
    private static int SEND_COUNT= 100000;
    private static int counter = 0;
    private static long startTime = 0;
    static {
        initCommChannel();
    }

    public void channelConnected(Channel ch)
    {
    }

    public void channelDisconnected(Channel ch)
    {
    }

    public void channelClosed(Channel ch)
    {
    }
    public void channelShunned()
    {
    }
    public void channelReconnected(Address addr)
    {
    }

    /**
     * this class initializes the communication channel to the broadcast
     * group defined, this is a two-way communication channel with full error-recovery
     * auto-resend capabilities and group auto-discovery built in it uses udp multi-cast, where multiple users can
     * all listen for broadcasts on the same port Everyone that's interested in these messages, just joins the group
     * and they will receive these messages
     */
    static private void initCommChannel() {
        // preload all the static ip's, we only do this once, of course
        String props = "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32;"+
                "ucast_recv_buf_size=16000;ucast_send_buf_size=16000;" +
                "mcast_send_buf_size=32000;mcast_recv_buf_size=64000;loopback=true):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" + "FD:" +
                "VERIFY_SUSPECT(timeout=1500):" + "pbcast.STABLE(desired_avg_gossip=10000):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=1000,1500,2000,3000;max_xmit_size=8192):" +
                "UNICAST(timeout=1000,1500,2000,3000):" +
                "FLOW_CONTROL(window_size=1000;fwd_mrgn=200;rttweight=0.125;reduction=0.75;expansion=1.25):"+
                "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";
        try {
            channel = new JChannel(props);
            HammerListener listener = new HammerListener();
            disp = new RpcDispatcher(channel, null, listener, listener);
            channel.connect("BOSGroup");

        }              
        catch (org.jgroups.ChannelException ce) {
            System.err.println("Channel Error"+ ce);
        }
    }

    static public int printnum(Integer number) throws Exception {
	counter ++;
	if(counter >=SEND_COUNT)
	    {
			long endTime = System.currentTimeMillis();
			System.out.println("Messages received "+counter);
			System.out.println("Messages succesfully trasmitted in "+(endTime-startTime));
			System.exit(0);
	    }
	return number.intValue() * 2;
    }

    public void viewAccepted(View new_view) {
        System.out.println("Accepted view (" + new_view.size() + new_view.getMembers() + ")");
    }

    public void suspect(Address suspected_mbr) {
        System.out.println("-- suspected " + suspected_mbr);
    }

    public void block() {
        ;
    }

    /** creates a new commandlistener and kick start's the thread */
    public HammerListener() {
        System.out.println("HammerListener loaded");
    }

    static public void main(String[] args) {
	startTime = System.currentTimeMillis();
	System.out.println("startTime "+startTime);
        HammerSender sender = new HammerSender();
	for(int i = 0;i<SEND_COUNT;i++) {
	    sender.executeDistributedCommand ("not used");
	}

    }

    /** lets' clean up when we are done */
    protected void finalizer() {
        channel.disconnect();
        channel.close();
    }

}

