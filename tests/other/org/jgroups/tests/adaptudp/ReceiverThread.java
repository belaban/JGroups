package org.jgroups.tests.adaptudp;

import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.List;

/**  Receiver thread: loops until it receives the expected
 *    number of messages. It measures the elapsed time between
 *    the reception of the first and the last message and
 *    calculates the throughputs. At the end, it closes the
 *    channel connection. (Actually the elapsed time refers
 *    to the reception of expected_msgs - 1, but that's not
 *    important.)
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class ReceiverThread extends Thread {
    private int msg_size;
    private int num_senders;
    private long expected_msgs;
    Logger  log=Logger.getLogger(this.getClass());
    long    counter=1;
    long    beginning=0, ending=0, elapsed_time, last_dump;
    long    log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");
    MulticastSocket recv_sock;
    List    receivers=new ArrayList();
    Object  counter_mutex=new Object();
    boolean started=false;


    public ReceiverThread(MulticastSocket recv_sock, int num_msgs, int ms, int ns, long log_interval) {
        msg_size=ms;
        num_senders=ns;
        expected_msgs=num_msgs * num_senders;
        this.log_interval=log_interval;
        this.recv_sock=recv_sock;
    }



    public void run() {
        double throughput_s, throughput_b;
        System.out.println("\nReceiver thread started...\n");
        counter=1;
        beginning=0;
        ending=0;
        boolean done=false;
        Request req;
        byte[] buf=new byte[300000];
        DatagramPacket p=new DatagramPacket(buf, buf.length);
        ByteArrayInputStream input;
        ObjectInputStream in;

        while(recv_sock != null && counter < expected_msgs && !done) {
            try {
                p.setData(buf);
                recv_sock.receive(p);
                input=new ByteArrayInputStream(p.getData(), 0, p.getLength());
                in=new ObjectInputStream(input);
                req=(Request)in.readObject();
                if(req.type != Request.DATA)
                    continue;

                synchronized(counter_mutex) {
                    if(counter == 1 && !started) {
                        beginning=System.currentTimeMillis();
                        last_dump=beginning;
                        started=true;
                    }
                    counter++;
                    if(counter % 100 == 0) {
                        System.out.println("-- received " + counter + " msgs");
                    }
                    if(counter % log_interval == 0) {
                        if(log.isInfoEnabled()) log.info(dumpStats(counter));
                    }
                    if(counter >= expected_msgs && !done) {
                        ending=System.currentTimeMillis();
                        done=true;
                    }
                }
            }
            catch(Exception ex) {
                if(recv_sock == null) return;
                break;
            }
        }


        if(gnuplot_output) {
            StringBuffer sb=new StringBuffer();
            sb.append("\n##### msgs_received");
            sb.append(", free_mem [KB] ");
            sb.append(", total_mem [KB] ");
            sb.append(", total_msgs_sec [msgs/sec] ");
            sb.append(", total_throughput [KB/sec] ");
            sb.append(", rolling_msgs_sec (last ").append(log_interval).append(" msgs) ");
            sb.append(" [msgs/sec] ");
            sb.append(", rolling_throughput (last ").append(log_interval).append(" msgs) ");
            sb.append(" [KB/sec]\n");
            if(log.isInfoEnabled()) log.info(sb.toString());
        }




        elapsed_time=(ending - beginning);

        System.out.println("expected_msgs=" + expected_msgs + ", elapsed_time=" + elapsed_time);

        throughput_s=expected_msgs / (elapsed_time/1000.0);
        throughput_b=(expected_msgs * (msg_size/1000.0)) / (elapsed_time/1000.0);

        String result="Received " + expected_msgs + " msgs. in " + elapsed_time + " msec.\n" +
                "Throughput: " + throughput_s + " [msgs/sec]\n" +
                "Throughput: " + throughput_b + " [KB/sec]\n" +
                "Total received: " + expected_msgs * (msg_size / 1000.0 / 1000.0) + " [MB]\n";
        System.out.println(result);
        if(log.isInfoEnabled()) log.info(result);
    }


    String dumpStats(long received_msgs) {
        StringBuffer sb=new StringBuffer();
        if(gnuplot_output)
            sb.append(received_msgs).append(' ');
        else
            sb.append("\nmsgs_received=").append(received_msgs);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().freeMemory() / 1000.0).append(' ');
        else
            sb.append(", free_mem=").append(Runtime.getRuntime().freeMemory() / 1000.0);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().totalMemory() / 1000.0).append(' ');
        else
            sb.append(", total_mem=").append(Runtime.getRuntime().totalMemory() / 1000.0).append('\n');

        dumpThroughput(sb, received_msgs);
        return sb.toString();
    }

    void dumpThroughput(StringBuffer sb, long received_msgs) {
        double tmp;
        long   current=System.currentTimeMillis();

        tmp=(1000 * counter) / (current - beginning);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else
            sb.append("total_msgs_sec=").append(tmp).append(" [msgs/sec]");

        tmp=(received_msgs * msg_size) / (current - beginning);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else
            sb.append("\ntotal_throughput=").append(tmp).append(" [KB/sec]");

        tmp=(1000 * log_interval) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else {
            sb.append("\nrolling_msgs_sec (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [msgs/sec]");
        }

        tmp=(log_interval * msg_size) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(' ');
        else {
            sb.append("\nrolling_throughput (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [KB/sec]\n");
        }
        last_dump=current;
    }

}
