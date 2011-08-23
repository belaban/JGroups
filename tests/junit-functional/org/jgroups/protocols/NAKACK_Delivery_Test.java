package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.stack.NakReceiverWindow;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests whether a mix of OOB and regular messages (with duplicates), sent my multiple threads, are delivery
 * correctly. Correct delivery means:
 * <ul>
 * <li>All messages are received exactly once (no duplicates and no missing messages)
 * <li>For regular messages only: all messages are received in the order in which they were sent (order of seqnos)
 * </ul>
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK_Delivery_Test {
    private NAKACK nak;
    private Address c1, c2;
    static final short NAKACK_ID=ClassConfigurator.getProtocolId(NAKACK.class);
    MyReceiver receiver=new MyReceiver();
    Executor pool;
    final static int NUM_MSGS=50;

    @BeforeMethod
    protected void setUp() throws Exception {
        c1=Util.createRandomAddress("C1");
        c2=Util.createRandomAddress("C2");
        nak=new NAKACK();

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new DefaultTimeScheduler(1);}
        };

        transport.setId((short)100);

        nak.setDownProtocol(transport);

        receiver.init(c1, c2);
        nak.setUpProtocol(receiver);

        nak.start();

        Vector<Address> members=new Vector<Address>(2);
        members.add(c1); members.add(c2);
        View view=new View(c1, 1, members);

        // set the local address
        nak.down(new Event(Event.SET_LOCAL_ADDRESS, c1));

        // set a dummy digest
        MutableDigest digest=new MutableDigest(2);
        digest.add(c1, 0, 0);
        digest.add(c2, 0, 0);
        nak.down(new Event(Event.SET_DIGEST, digest));

        // set dummy view
        nak.down(new Event(Event.VIEW_CHANGE, view));

        pool=new ThreadPoolExecutor(1, 100, 1000, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());
        // pool=new DirectExecutor();
        // if(pool instanceof ThreadPoolExecutor)
        ((ThreadPoolExecutor)pool).setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }


    @AfterMethod
    protected void tearDown() {
        nak.stop();
        if(pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)pool).shutdownNow();
    }


    /**
     * Sends NUM_MSGS (regular or OOB) multicasts on c1 and c2, checks that both c1 and c2 received NUM_MSGS messages.
     * This test doesn't use a transport, but injects messages directly into NAKACK.
     */
    public void testSendingOfRandomMessages() {
        List<Integer> seqnos=generateRandomNumbers(1, NUM_MSGS);
        seqnos.addAll(generateRandomNumbers(1, NUM_MSGS));
        seqnos.addAll(generateRandomNumbers(Math.min(15, NUM_MSGS / 2), NUM_MSGS / 2));
        seqnos.addAll(generateRandomNumbers(2, NUM_MSGS));
        seqnos.addAll(generateRandomNumbers(5, Math.max(5, NUM_MSGS - 10)));

        Set<Integer> no_duplicates=new HashSet<Integer>(seqnos);

        System.out.println("sending " + seqnos.size() + " msgs (including duplicates); size excluding duplicates=" +
                no_duplicates.size());

        // we need to add our own messages (nak is for C1), or else they will get discarded by NAKACK.handleMessage()
        NakReceiverWindow win=nak.getWindow(c1);
        for(int i=1; i <= NUM_MSGS; i++)
            win.add(i, msg(c1, i, i, true));

        for(int i: seqnos) {
            boolean oob=Util.tossWeightedCoin(0.5);
            pool.execute(new Sender(c2, i, i, oob));
            pool.execute(new Sender(c1, i, i, oob));
        }

        ConcurrentMap<Address, Collection<Message>> msgs=receiver.getMsgs();
        Collection<Message> c1_list=msgs.get(c1);
        Collection<Message> c2_list=msgs.get(c2);

        long end_time=System.currentTimeMillis() + 10000;
        while(System.currentTimeMillis() < end_time) {
            int size_c1=c1_list.size();
            int size_c2=c2_list.size();
            System.out.println("size C1 = " + size_c1 + ", size C2=" + size_c2);
            if(size_c1 == NUM_MSGS && size_c2 == NUM_MSGS)
                break;
            Util.sleep(1000);
        }

        assert c1_list.size() == NUM_MSGS : "[C1] expected " + NUM_MSGS + " messages, but got " + c1_list.size();
        assert c2_list.size() == NUM_MSGS : "[C2] expected " + NUM_MSGS + " messages, but got " + c2_list.size();
    }

    private static List<Integer> generateRandomNumbers(int from, int to) {
        List<Integer> retval=new ArrayList<Integer>(20);
        for(int i=from; i <= to; i++)
            retval.add(i);
        Collections.shuffle(retval);
        return retval;
    }


    private void send(Address sender, long seqno, int number, boolean oob) {
        assert sender != null;
        nak.up(new Event(Event.MSG, msg(sender, seqno, number, oob)));
    }


    private static Message msg(Address sender, long seqno, int number, boolean oob) {
        Message msg=new Message(null, sender, number);
        if(oob)
            msg.setFlag(Message.OOB);
        if(seqno != -1)
            msg.putHeader(NAKACK_ID, NakAckHeader.createMessageHeader(seqno));
        return msg;
    }


    static class MyReceiver extends Protocol {
        final ConcurrentMap<Address, Collection<Message>> msgs=new ConcurrentHashMap<Address,Collection<Message>>();

        public ConcurrentMap<Address, Collection<Message>> getMsgs() {
            return msgs;
        }
        public void init(Address ... mbrs) {
            for(Address mbr: mbrs) {
                msgs.putIfAbsent(mbr, new ConcurrentLinkedQueue<Message>());
            }
        }

        public Object up(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                Address sender=msg.getSrc();
                Collection<Message> list=msgs.get(sender);
                if(list == null) {
                    list=new ConcurrentLinkedQueue<Message>();
                    Collection<Message> tmp=msgs.putIfAbsent(sender, list);
                    if(tmp != null)
                        list=tmp;
                }
                list.add(msg);
            }
            return null;
        }
    }

    class Sender implements Runnable {
        final Address sender;
        final long seqno;
        final int number;
        final boolean oob;

        public Sender(Address sender, long seqno, int number, boolean oob) {
            this.sender=sender;
            this.seqno=seqno;
            this.number=number;
            this.oob=oob;
        }

        public void run() {
            send(sender, seqno, number, oob);
        }
    }

}