package org.jgroups.tests;

import org.jgroups.Message;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.UNICAST;

/**
 * @author Bela Ban
 * @version $Id: MessageStressTest.java,v 1.3 2008/07/29 12:08:31 belaban Exp $
 */
public class MessageStressTest {
    public static final int NUM=1000 * 1000;
    public static final String UDP="UDP";
    public static final String NAKACK="NAKACK";
    public static final String UNICAST="UNICAST";
    int cnt=NUM / 10;
    

    private static void start() {
        Message msg;
        long start;
        start=System.nanoTime();
        for(int i=0; i < NUM; i++) {
            //if(i % cnt == 0)
              //  System.out.println(i);
            msg=new Message(); // creates 'headers' hashmap
            msg.putHeader(NAKACK, new NakAckHeader(NakAckHeader.MSG, (long)i));
            msg.putHeader(UNICAST, new UNICAST.UnicastHeader((byte)0, i));
            msg.putHeader(UDP, new TpHeader("demo"));
            msg.getHeader(UDP);
            msg.getHeader(NAKACK);
            msg.getHeader(UNICAST);
            msg.size();
        }
        long total=System.nanoTime() - start;
        double ns_per_msg=total / NUM;
        double us_per_msg=ns_per_msg / 1000L;
        double ms_per_msg=us_per_msg / 1000L;

        double msgs_per_ns=NUM / (double)total;
        double msgs_per_us=msgs_per_ns * 1000L;
        double msgs_per_ms=msgs_per_us * 1000L;
        double msgs_per_sec=msgs_per_ms * 1000L;

        System.out.println(NUM + " msgs, total time: " + total + " ns; " +
                ns_per_msg + " ns/msg, " + us_per_msg + " microsecs/msg, " + ms_per_msg + " ms/msg");
        System.out.println(msgs_per_ns + " messages/ns, " + msgs_per_us + " msgs/us, " + msgs_per_ms + " msgs/ms, " +
        msgs_per_sec + " msgs/sec");
    }

    public static void main(String[] args) {
        MessageStressTest.start();
    }


}
