package org.jgroups.tests.adapt;

import org.apache.log4j.Logger;
import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.Message;

/**  Sender thread: inputs into the system a num_busts bursts
 *    of msgs_burst messages composed of msg_size bytes.
 *    Sleeps for sleep_msec after each burst.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class SenderThread extends Thread {

    private Channel channel;
    private int num_msgs;
    private int msg_size;
    Logger  log=Logger.getLogger(this.getClass());
    long log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");

    public SenderThread(Channel ch, int num_msgs, int msg_size, long log_interval) {
        channel=ch;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        this.log_interval=log_interval;
    }

    public void run() {
        long total_msgs=0;

        System.out.println("Sender thread started...");

        try {
            byte[] msg=new byte[msg_size];
            for(int h=0; h < msg_size; h++) {
                msg[h]=(byte)h;
            }

            for(int i=0; i < num_msgs; i++) {
                Message jg_msg=new Message(null, null, msg);
                channel.send(jg_msg);
                total_msgs++;
                if(total_msgs % 1000 == 0) {
                    System.out.println("++ sent " + total_msgs);
                }
                if(total_msgs % log_interval == 0) {
                    if(gnuplot_output == false)
                        if(log.isInfoEnabled()) log.info(dumpStats(total_msgs));
                }
            }
            System.out.println("Sent all messages. Sender terminates.\n");
        }
        catch(ChannelNotConnectedException e) {
            e.printStackTrace();
        }
        catch(ChannelClosedException e) {
            e.printStackTrace();
        }
    }


    String dumpStats(long sent_msgs) {
        StringBuffer sb=new StringBuffer();
        sb.append("\nmsgs_sent=").append(sent_msgs).append('\n');
        sb.append("free_mem=").append(Runtime.getRuntime().freeMemory());
        sb.append(" (total_mem=").append(Runtime.getRuntime().totalMemory()).append(")\n");
        return sb.toString();
    }
}
