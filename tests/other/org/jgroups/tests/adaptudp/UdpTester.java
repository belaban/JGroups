package org.jgroups.tests.adaptudp;

import java.net.ServerSocket;
import java.net.DatagramSocket;
import java.net.MulticastSocket;
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
public class UdpTester {
    private boolean sender;
    private int num_msgs;
    private int msg_size;
    private int num_senders;
    private long log_interval=1000;
    MulticastSocket recv_sock;
    DatagramSocket send_sock;
    int num_members;


    public UdpTester(MulticastSocket recv_sock, DatagramSocket send_sock, boolean snd, int num_msgs,
                     int msg_size, int num_members, int ns, long log_interval) {
        sender=snd;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        num_senders=ns;
        this.num_members=num_members;
        this.log_interval=log_interval;
        this.recv_sock=recv_sock;
        this.send_sock=send_sock;
    }

    public void initialize() {
        new ReceiverThread(recv_sock, num_msgs, msg_size, num_senders, log_interval).start();
        if(sender) {
            new SenderThread(send_sock, num_msgs, msg_size, log_interval).start();
        }
    }
}
