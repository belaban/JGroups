package org.jgroups.tests.adapt;

import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.Message;
import org.apache.log4j.Logger;

/**  Sender thread: inputs into the system a num_busts bursts
 *    of msgs_burst messages composed of msg_size bytes.
 *    Sleeps for sleep_msec after each burst.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class SenderThread extends Thread {

    private Channel channel;
    private long msgs_burst;
    private long sleep_msec;
    private int num_bursts;
    private int msg_size;
    Logger  log=Logger.getLogger(this.getClass());
    long log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");

    public SenderThread(Channel ch, long mb, long st, int nb, int ms, long log_interval) {
        channel=ch;
        msgs_burst=mb;
        sleep_msec=st;
        num_bursts=nb;
        msg_size=ms;
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

            for(int i=0; i < num_bursts; i++) {
                for(int j=0; j < msgs_burst; j++) {
                    Message jg_msg=new Message(null, null, msg);
                    channel.send(jg_msg);
                    total_msgs++;
                    if(total_msgs % 100 == 0) {
                        System.out.println("++ sent " + total_msgs);
                    }
                    if(total_msgs % log_interval == 0) {
                        if(gnuplot_output == false)
                            log.info(dumpStats(total_msgs));
                    }
                }
                sleep((long)sleep_msec);
            }
            System.out.println("Sent all bursts. Sender terminates.\n");
        }
        catch(ChannelNotConnectedException e) {
            e.printStackTrace();
        }
        catch(ChannelClosedException e) {
            e.printStackTrace();
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
    }


    String dumpStats(long sent_msgs) {
        StringBuffer sb=new StringBuffer();
        sb.append("\nmsgs_sent=").append(sent_msgs).append("\n");
        sb.append("free_mem=").append(Runtime.getRuntime().freeMemory());
        sb.append(" (total_mem=").append(Runtime.getRuntime().totalMemory()).append(")\n");
        return sb.toString();
    }
}
