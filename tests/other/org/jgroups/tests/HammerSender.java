package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;

import java.io.InputStream;

/*
 * @author Bob Stevenson - HAMMER
 * @author Ananda Bollu - FLOW_CONTROL
 */
public class HammerSender {
    private String scriptCommand;
    private int port;
    private boolean directCommand;
    private String machineName;
    private String localFilename;
    private InputStream localInputStream;
    private static JChannel channel = null;

    private static RpcDispatcher disp;
    private static int count = 0;

    private static String props =  "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=64;"+
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

    private static MethodCall printnumMethod = null;

    static {
        initCommChannel();
        loadMethods();
    }

    private static void  loadMethods()
    {
        try {
            java.lang.reflect.Method method = HammerListener.class.getMethod("printnum",new Class[] { Integer.class });
            printnumMethod = new org.jgroups.blocks.MethodCall( method );
            printnumMethod.addArg(new Integer(2));

        }

        catch(java.lang.NoSuchMethodException nsme)
        {
            System.err.println("No Such method:"+ nsme);
        }
        catch(Exception e)
        {
            System.err.println("Error:"+ e);
        }
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

        try {
            channel = new JChannel(props);
	    System.out.println(channel.getProtocolStack().printProtocolSpec(false));
            disp = new RpcDispatcher(channel, null, null, null);
            channel.connect("BOSGroup");
        }
        catch (org.jgroups.ChannelException ce) {
            System.err.println("Channel Error"+ ce);
        }
    }



    /**
     * executes a command across app-servers
     * @param c the command to execute across boxes in an environment
     */
    public static void executeDistributedCommand(String cmd) {

        disp.callRemoteMethods(null, printnumMethod, GroupRequest.GET_NONE, 0);
    }

    /**
     * this method will close down the channel forcing out and data, and removing ourselves
     * from further participation in the group
     */
    static void shutdown() {
        //
        disp.stop();
        channel.close();
    }
}

