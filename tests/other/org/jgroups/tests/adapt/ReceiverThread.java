package org.jgroups.tests.adapt;

import org.jgroups.*;
import org.apache.log4j.Logger;

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

    private Channel channel;
    private int num_msgs;
    private int msg_size;
    private int num_senders;
    Logger  log=Logger.getLogger(this.getClass());
    long    counter=1;
    long    expected_msgs=num_msgs * num_senders;
    long    beginning=0, ending=0, elapsed_time, last_dump;
    long    log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");


    public ReceiverThread(Channel ch, int num_msgs, int msg_size, int ns, long log_interval) {
        channel=ch;
        this.num_msgs=num_msgs;
        this.msg_size=msg_size;
        num_senders=ns;
        expected_msgs=num_msgs * num_senders;
        this.log_interval=log_interval;
    }

    public void run() {
        double throughput_s, throughput_b;
        System.out.println("\nReceiver thread started...\n");
        counter=1;
        beginning=0;
        ending=0;

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
            log.info(sb.toString());
        }


        while(counter <= expected_msgs) {
            try {
                Object received=channel.receive(60000);
                if(received instanceof Message) {
                    if(counter == 1) {
                        beginning=System.currentTimeMillis();
                        last_dump=beginning;
                    }
                    if(counter == expected_msgs)
                        ending=System.currentTimeMillis();
                    counter++;
                    if(counter % 1000 == 0) {
                        System.out.println("-- received " + counter + " msgs");
                    }
                    if(counter % log_interval == 0) {
                        log.info(dumpStats(counter));
                    }
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
                ending=System.currentTimeMillis();
                System.out.println("Received " + counter + " / " +
                        expected_msgs + " messages");
                expected_msgs=counter;
                break;
            }
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
        log.info(result);

        //String xmit_stats=NAKACK.dumpXmitStats();
        //Trace.info("TRACE.special()", "stats:\n\n" + xmit_stats);

        long sleep_time=10000;

        System.out.println("-- sleeping for " + (sleep_time / 1000) + " seconds to allow for retransmissions");
        try {
            Thread.sleep(sleep_time);
        }
        catch(Throwable t) {
        }

        channel.close();    // Apparently does not work properly?
        System.exit(0);       // Radical alternative to channel.close().
    }


    String dumpStats(long received_msgs) {
        StringBuffer sb=new StringBuffer();
        if(gnuplot_output)
            sb.append(received_msgs).append(" ");
        else
            sb.append("\nmsgs_received=").append(received_msgs);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().freeMemory() / 1000.0).append(" ");
        else
            sb.append(", free_mem=").append(Runtime.getRuntime().freeMemory() / 1000.0);

        if(gnuplot_output)
            sb.append(Runtime.getRuntime().totalMemory() / 1000.0).append(" ");
        else
            sb.append(", total_mem=").append(Runtime.getRuntime().totalMemory() / 1000.0).append("\n");

        dumpThroughput(sb, received_msgs);
        return sb.toString();
    }

    void dumpThroughput(StringBuffer sb, long received_msgs) {
        double tmp;
        long   current=System.currentTimeMillis();

        tmp=(1000 * counter) / (current - beginning);
        if(gnuplot_output)
            sb.append(tmp).append(" ");
        else
            sb.append("total_msgs_sec=").append(tmp).append(" [msgs/sec]");

        tmp=(received_msgs * msg_size) / (current - beginning);
        if(gnuplot_output)
            sb.append(tmp).append(" ");
        else
            sb.append("\ntotal_throughput=").append(tmp).append(" [KB/sec]");

        tmp=(1000 * log_interval) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(" ");
        else {
            sb.append("\nrolling_msgs_sec (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [msgs/sec]");
        }

        tmp=(log_interval * msg_size) / (current - last_dump);
        if(gnuplot_output)
            sb.append(tmp).append(" ");
        else {
            sb.append("\nrolling_throughput (last ").append(log_interval).append(" msgs)=");
            sb.append(tmp).append(" [KB/sec]\n");
        }
        last_dump=current;
    }
}
