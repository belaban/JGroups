package org.jgroups.tests.adapt;

import org.jgroups.*;



/**  Javagroups version used was 2.0.3. Recompiled and tested again with 2.0.6.
 *   JGroupsTester:
 *   1. Instantiates a JChannel object and joins the group.
 *       Partition properties conf. is the same as in the JBoss
 *       default configuration except for min_wait_time parameter
 *       that causes the following error:
 *			UNICAST.setProperties():
 *			these properties are not recognized:
 *			-- listing properties --
 *			   min_wait_time=2000
 *   2. Starts receiving until it receives a view change message
 *       with the expected number of members.
 *   3. Starts the receiver thread and if(sender), the sender thread.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)
 */
public class JGroupsTester {

    /*
       private String props="UDP(mcast_addr=228.1.2.3;mcast_port=45566):" +
                            "PING:" +
                            "FD(timeout=5000):" +
                            "VERIFY_SUSPECT(timeout=1500):" +
                            "MERGE:" +
                            "NAKACK:" +
                            "UNICAST(timeout=5000):" +
                            //"UNICAST(timeout=5000;min_wait_time=2000):"
                            "FRAG:" +
                            "FLUSH:" +
                            "GMS:" +
                            "STATE_TRANSFER:" +
                            "QUEUE";
    */



    private String props="UDP(mcast_recv_buf_size=64000;mcast_send_buf_size=32000;mcast_port=45566;use_packet_handler=true;ucast_recv_buf_size=64000;mcast_addr=228.8.8.8;loopback=true;ucast_send_buf_size=32000;ip_ttl=32):AUTOCONF:PING(timeout=2000;num_initial_members=3):MERGE2(max_interval=10000;min_interval=5000):FD(timeout=2000;max_tries=3;shun=true):VERIFY_SUSPECT(timeout=1500):pbcast.NAKACK(max_xmit_size=8192;gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):UNICAST(timeout=300,600,1200,2400,3600):pbcast.STABLE(stability_delay=1000;desired_avg_gossip=5000;max_bytes=250000):pbcast.GMS(print_local_addr=true;join_timeout=3000;join_retry_timeout=2000;shun=true):FC(max_credits=2000000;down_thread=false;direct_blocking=true;min_credits=52000):FRAG(frag_size=8192;down_thread=false;up_thread=true)";


    private JChannel channel;
    private View view;
    private String myGrpName="myGroup";
    private boolean sender;
    private long msgs_burst;
    private long sleep_msec;
    private int num_bursts;
    private int msg_size;
    private int grpMembers;
    private int num_senders;
    private long log_interval=1000;


    public JGroupsTester(boolean snd, long mb, long st,
                            int nb, int ms, int gm, int ns, String props, long log_interval) {
        sender=snd;
        msgs_burst=mb;
        sleep_msec=st;
        num_bursts=nb;
        msg_size=ms;
        grpMembers=gm;
        num_senders=ns;
        if(props != null)
            this.props=props;
        this.log_interval=log_interval;

        System.out.println("props=" + this.props);
    }

    public void initialize() {

        try {
            channel=new JChannel(props);
            // Debugger d=new Debugger(channel, false);
            // d.start();
            channel.connect(myGrpName);
        }
        catch(ChannelException e) {
            e.printStackTrace();
        }

        boolean loop=true;
        while(loop) {
            try {
                view=(View)channel.receive(0);
                System.out.println("-- view: " + view.getMembers());
                if(view.size() >= grpMembers) {
                    loop=false;
                    System.out.println(
                            "Everyone joined, ready to begin test...");
                }
            }
            catch(ClassCastException e) {
                continue;
            }
            catch(ChannelNotConnectedException e) {
                e.printStackTrace();
            }
            catch(ChannelClosedException e) {
                e.printStackTrace();
            }
            catch(TimeoutException e) {
                e.printStackTrace();
            }

        }

        new ReceiverThread(channel, msgs_burst, num_bursts,
                msg_size, num_senders, log_interval).start();
        if(sender) {
            new SenderThread(channel, msgs_burst, sleep_msec,
                    num_bursts, msg_size, log_interval).start();
        }
    }
}
