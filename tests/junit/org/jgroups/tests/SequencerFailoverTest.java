

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;


/**
 * Tests a SEQUENCER based stack: A, B and C. B starts multicasting messages with a monotonically increasing
 * number. Then A is crashed. C and B should receive *all* numbers *without* a gap.
 * @author Bela Ban
 * @version $Id: SequencerFailoverTest.java,v 1.1 2006/01/03 15:38:02 belaban Exp $
 */
public class SequencerFailoverTest extends TestCase {
    JChannel ch1, ch2, ch3; // ch1 is the coordinator
    final String GROUP="demo-group";
    final int NUM_MSGS=50;


    String props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;" +
            "enable_bundling=true;use_incoming_packet_handler=true;loopback=true):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD(timeout=2000;max_tries=2):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true):" +
            "SEQUENCER";



    public SequencerFailoverTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        ch1=new JChannel(props);
        ch1.connect(GROUP);

        ch2=new JChannel(props);
        ch2.connect(GROUP);

        ch3=new JChannel(props);
        ch3.connect(GROUP);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(ch3 != null) {
            ch3.close();
            ch3 = null;
        }
        if(ch2 != null) {
            ch2.close();
            ch2 = null;
        }
    }

    public void testBroadcastSequence() throws Exception {
        new Thread() {
            public void run() {
                Util.sleepRandom(100);
                ch1.shutdown(); ch1=null;
            }
        }.start();

        for(int i=1; i <= NUM_MSGS; i++) {
            Util.sleep(300);
            ch2.send(new Message(null, null, new Integer(i)));
            System.out.print("-- messages sent: " + i + "/" + NUM_MSGS + "\r");
        }
        System.out.println("");
        View view=ch2.getView();
        System.out.println("ch2's view is " + view);
        assertEquals(2, view.getMembers().size());
        Util.sleep(5000);
        System.out.println("-- verifying messages on ch2 and ch3");
        verifyNumberOfMessages(NUM_MSGS, ch2);
        verifyNumberOfMessages(NUM_MSGS, ch3);
    }

    private void verifyNumberOfMessages(int num_msgs, JChannel ch) throws Exception {
        List msgs=getMessages(ch);
        System.out.println("list has " + msgs.size() + " msgs (should have " + NUM_MSGS + ")");
        assertEquals(num_msgs, msgs.size());
        int tmp, i=1;
        for(Iterator it=msgs.iterator(); it.hasNext();) {
            tmp=((Integer)it.next()).intValue();
            if(tmp != i)
                throw new Exception("expected " + i + ", but got " + tmp);
            i++;
        }
    }

    private List getMessages(JChannel ch) throws Exception {
        List retval=new LinkedList();
        Object obj;
        while(ch.getNumMessages() > 0) {
            obj=ch.receive(1000);
            if(obj instanceof Message) {
                Message msg=(Message)obj;
                retval.add(msg.getObject());
            }
        }
        return retval;
    }


    public static void main(String[] args) {
        String[] testCaseName={SequencerFailoverTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
