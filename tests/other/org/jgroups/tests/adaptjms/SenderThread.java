package org.jgroups.tests.adaptjms;

import org.apache.log4j.Logger;

import javax.jms.ObjectMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import java.util.List;

/**  Sender thread: inputs into the system a num_busts bursts
 *    of msgs_burst messages composed of msg_size bytes.
 *    Sleeps for sleep_msec after each burst.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class SenderThread extends Thread {
    private int num_msgs;
    private int msg_size;
    Logger  log=Logger.getLogger(this.getClass());
    long log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");
    List nodes;
    TopicPublisher pub;
    TopicSession session;
    Topic topic;


    public SenderThread(TopicSession session, TopicPublisher pub, Topic topic, int num_msgs, int ms, long log_interval) {
        this.num_msgs=num_msgs;
        msg_size=ms;
        this.log_interval=log_interval;
        this.session=session;
        this.pub=pub;
        this.topic=topic;
    }

    public void run() {
        long total_msgs=0;
        Request req;
        ObjectMessage msg;

        System.out.println("Sender thread started...");

        try {
            byte[] m=new byte[msg_size];
            for(int h=0; h < msg_size; h++) {
                m[h]=(byte)h;
            }

            System.out.println("Everyone joined, ready to begin test...\n");

            for(int i=0; i < num_msgs; i++) {
                req=new Request(Request.DATA, m);
                msg=session.createObjectMessage(req);
                pub.publish(topic, msg);

                total_msgs++;
                if(total_msgs % 1000 == 0) {
                    System.out.println("++ sent " + total_msgs);
                }
                if(total_msgs % log_interval == 0) {
                    if(gnuplot_output == false)
                        if(log.isInfoEnabled()) log.info(dumpStats(total_msgs));
                }
            }
            System.out.println("Sent all bursts. Sender terminates.\n");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
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
