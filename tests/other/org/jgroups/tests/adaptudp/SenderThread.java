package org.jgroups.tests.adaptudp;

import org.apache.log4j.Logger;
import org.jgroups.util.Util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
    DatagramSocket send_sock;



    public SenderThread(DatagramSocket send_sock, int num_msgs, int ms, long log_interval) {
        this.num_msgs=num_msgs;
        msg_size=ms;
        this.log_interval=log_interval;
        this.send_sock=send_sock;
    }

    public void run() {
        long total_msgs=0;
        Request req;
        byte[] buf;
        DatagramPacket p;
        InetAddress mcast_addr;

        System.out.println("Sender thread started...");

        try {
            mcast_addr=InetAddress.getByName(Test.mcast_addr);

            byte[] msg=new byte[msg_size];
            for(int h=0; h < msg_size; h++) {
                msg[h]=(byte)h;
            }

            System.out.println("Everyone joined, ready to begin test...\n");

            for(int i=0; i < num_msgs; i++) {
                req=new Request(Request.DATA, msg);
                buf=Util.objectToByteBuffer(req);
                p=new DatagramPacket(buf, buf.length, mcast_addr, Test.mcast_port);
                send_sock.send(p);
                total_msgs++;
                if(total_msgs % 1000 == 0) {
                    System.out.println("++ sent " + total_msgs);
                }
                if(total_msgs % log_interval == 0) {
                    if(gnuplot_output == false)
                        log.info(dumpStats(total_msgs));
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
