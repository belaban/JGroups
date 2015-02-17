package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PRIO;
import org.jgroups.protocols.PrioHeader;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * @author Bela Ban
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED},singleThreaded=true)
public class PrioTest extends ChannelTestBase {
    protected JChannel c1, c2;
    protected PrioReceiver r1, r2;
    protected static final short PRIO_ID=ClassConfigurator.getProtocolId(PRIO.class);

    @BeforeTest void init() throws Exception {
        c1=createChannel(true, 2, "A");
        c1.getProtocolStack().insertProtocol(new PRIO(), ProtocolStack.ABOVE, NAKACK2.class);
        c2=createChannel(c1, "B");
        c1.connect("PrioTest");
        c1.setReceiver(r1=new PrioReceiver());
        c2.connect("PrioTest");
        c2.setReceiver(r2=new PrioReceiver());
        assert c2.getView().size() == 2;
    }

    @AfterTest void destroy() {
        Util.close(c2, c1);
    } 


    public void testPrioritizedMessages() throws Exception {
        byte[] prios={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30};
        PrioSender[] senders=new PrioSender[prios.length];
        final CyclicBarrier barrier=new CyclicBarrier(prios.length +1);
        for(int i=0; i < prios.length; i++) {
            senders[i]=new PrioSender(c1, prios[i], barrier);
            senders[i].start();
        }
        Util.sleep(500);
        barrier.await(); // starts the senders

        for(PrioSender sender: senders)
            sender.join(10000);
        for(PrioSender sender: senders)
            if(sender.isAlive())
                System.err.println("sender " + sender + " is still alive");

        List<Integer> list1=r1.getMsgs(), list2=r2.getMsgs();
        for(int i=0; i < 20; i++) {
            if(list1.size() == prios.length && list2.size() == prios.length)
                break;
            Util.sleep(1000);
        }

        System.out.println("R1: " + Util.print(list1) + "\nR2: " + Util.print(list2));
        assert list1.size() == prios.length;
        assert list2.size() == prios.length;
        
        // Mike: how can we test this ? It seems this is not deterministic... which is fine, I guess, but hard to test !
        checkOrdering(list1, list2);
    }

    /**
     * Verifies that the messages are predominantly in prioritized order.
     * The latter means that messages with higher prio should be mostly delivered before messages with lower prio.
     * 'Mostly' is needed because we cannot guarantee that all messages with higher prio are delivered before messages
     * with lower prio. 
     * @param list1
     * @param list2
     */
    protected void checkOrdering(List<Integer> list1, List<Integer> list2) {
        // check that the left half of the messages have a higher prio than the right half
        System.out.print("checking the ordering of list1: ");
        _check(list1);

        System.out.print("checking the ordering of list2: ");
        _check(list2);
    }

    protected void _check(List<Integer> list) {
        int middle=list.size() / 2;

        int sum=0;
        for(int num: list)
          sum+=num;

        double median_val=sum / list.size();


        // make sure the values [0 .. middle] have smaller values than list[middle]
        int correct=0;
        for(int i=0; i <= middle; i++) {
            if(list.get(i) <= median_val)
                correct++;
        }

        // make sure the values [middle+1 .. list.size() -1] have bigger values than list[middle+1]
        for(int i=middle+1; i < list.size() -1; i++) {
            if(list.get(i) >= median_val)
                correct++;
        }

        double correct_percentage=correct / (double)list.size();
        System.out.println("OK. The percentage of correct values is " + (correct_percentage * 100) + "%");
        assert correct_percentage >= 0.7 : "FAIL. The percentage of correct values is " + (correct_percentage * 100) + "%";
    }


    protected static class PrioSender extends Thread {
        protected final JChannel ch;
        protected final byte prio;
        protected final CyclicBarrier barrier;

        public PrioSender(JChannel ch, byte prio, CyclicBarrier barrier) {
            this.ch=ch;
            this.prio=prio;
            this.barrier=barrier;
        }

        public void run() {
            Message msg=new Message(null, null,(int)prio);
            PrioHeader hdr=new PrioHeader(prio);
            msg.putHeader(PRIO_ID, hdr);
            try {
                barrier.await();
                ch.send(msg);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    
    protected static class PrioReceiver extends ReceiverAdapter {
        protected final List<Integer> msgs=new LinkedList<>();

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            msgs.add((Integer)msg.getObject());
        }
    }
}
