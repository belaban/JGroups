package org.jgroups.tests.adapttcp;

import org.apache.log4j.Logger;
import org.jgroups.util.Util;

import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

/**  Sender thread: inputs into the system a num_busts bursts
 *    of msgs_burst messages composed of msg_size bytes.
 *    Sleeps for sleep_msec after each burst.
 * @author Milcan Prica (prica@deei.units.it)
 * @author Bela Ban (belaban@yahoo.com)

 */
public class SenderThread extends Thread {

    private long msgs_burst;
    private long sleep_msec;
    private int num_bursts;
    private int msg_size;
    Logger  log=Logger.getLogger(this.getClass());
    long log_interval=1000;
    boolean gnuplot_output=Boolean.getBoolean("gnuplot_output");
    List nodes;



    public SenderThread(List nodes, long mb, long st, int nb, int ms, long log_interval) {
        msgs_burst=mb;
        sleep_msec=st;
        num_bursts=nb;
        msg_size=ms;
        this.log_interval=log_interval;
        this.nodes=nodes;
    }

    public void run() {
        long total_msgs=0;
        ConnectionTable ct=null;


        System.out.println("Sender thread started...");

        try {
            ct=new ConnectionTable(nodes);
            byte[] msg=new byte[msg_size];
            for(int h=0; h < msg_size; h++) {
                msg[h]=(byte)h;
            }

            while(true) {
                try {
                    ct.init();
                    break;
                }
                catch(Exception ex) {
                    Util.sleep(1000);
                }
            }
            System.out.println("Everyone joined, ready to begin test...\n" +
                    "cluster: " + ct.toString());

            for(int i=0; i < num_bursts; i++) {
                for(int j=0; j < msgs_burst; j++) {
                    ct.writeMessage(msg);
                    total_msgs++;
                    if(total_msgs % 1000 == 0) {
                        System.out.println("++ sent " + total_msgs);
                    }
                    if(total_msgs % log_interval == 0) {
                        if(gnuplot_output == false)
                            log.info(dumpStats(total_msgs));
                    }
                }
                sleep(sleep_msec);
            }
            System.out.println("Sent all bursts. Sender terminates.\n");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if(ct != null)
                ct.close();
        }
    }



    String dumpStats(long sent_msgs) {
        StringBuffer sb=new StringBuffer();
        sb.append("\nmsgs_sent=").append(sent_msgs).append("\n");
        sb.append("free_mem=").append(Runtime.getRuntime().freeMemory());
        sb.append(" (total_mem=").append(Runtime.getRuntime().totalMemory()).append(")\n");
        return sb.toString();
    }

    class ConnectionTable {
        List nodes;
        Connection[] connections;

        ConnectionTable(List nodes) throws Exception {
            this.nodes=nodes;
            connections=new Connection[nodes.size()];
        }


        void init() throws Exception {
            int i=0;

            for(Iterator it=nodes.iterator(); it.hasNext();) {
                InetAddress addr=(InetAddress)it.next();
                if(connections[i] == null) {
                    connections[i]=new Connection(addr);
                    System.out.println("-- connected to " +addr);
                    System.out.flush();
                }
                i++;
            }
        }

        // todo: parallelize
        void writeMessage(byte[] msg) throws Exception {
            for(int i=0; i < connections.length; i++) {
                Connection c=connections[i];
                if(c != null)
                    c.writeMessage(msg);
            }
        }

        void close() {
            for(int i=0; i < connections.length; i++) {
                Connection c=connections[i];
                if(c != null)
                    c.close();
            }
        }

        public String toString() {
            StringBuffer sb=new StringBuffer();
            for(Iterator it=nodes.iterator(); it.hasNext();) {
                InetAddress inetAddress=(InetAddress)it.next();
                sb.append(inetAddress).append(" ");
            }
            return sb.toString();
        }
    }

    class Connection {
        Socket sock;
        DataOutputStream out;

        Connection(InetAddress addr) throws Exception {
            sock=new Socket(addr, Test.srv_port);
            out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        }

        void writeMessage(byte[] msg) throws Exception {
            out.writeInt(msg.length);
            out.write(msg, 0, msg.length);
            out.flush();
        }

        void close() {
            try {
                sock.close();
            }
            catch(Exception ex) {

            }
        }
    }

}
