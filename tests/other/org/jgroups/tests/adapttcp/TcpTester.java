package org.jgroups.tests.adapttcp;

import java.net.ServerSocket;
import java.util.List;



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
public class TcpTester {
    private boolean sender;
    private long msgs_burst;
    private long sleep_msec;
    private int num_bursts;
    private int msg_size;
    private int grpMembers;
    private int num_senders;
    private long log_interval=1000;
    ServerSocket srv_sock;
    List nodes;


    public TcpTester(boolean snd, long mb, long st,
                            int nb, int ms, int gm, int ns, long log_interval,
                            ServerSocket srv_sock, List nodes) {
        sender=snd;
        msgs_burst=mb;
        sleep_msec=st;
        num_bursts=nb;
        msg_size=ms;
        grpMembers=gm;
        num_senders=ns;
        this.log_interval=log_interval;
        this.srv_sock=srv_sock;
        this.nodes=nodes;
    }

    public void initialize() {
        new ReceiverThread(srv_sock, msgs_burst, num_bursts,
                msg_size, num_senders, log_interval).start();
        if(sender) {
            new SenderThread(nodes, msgs_burst, sleep_msec,
                    num_bursts, msg_size, log_interval).start();
        }
    }
}
