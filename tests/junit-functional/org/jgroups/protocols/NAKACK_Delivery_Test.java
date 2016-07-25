package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests whether a mix of OOB and regular messages (with duplicates), sent my multiple threads, are delivered
 * correctly. Correct delivery means:
 * <ul>
 * <li>All messages are received exactly once (no duplicates and no missing messages)
 * <li>For regular messages only: all messages are received in the order in which they were sent (order of seqnos)
 * </ul>
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK_Delivery_Test {
    protected NAKACK2            nak;
    protected Address            a, b;
    protected MyReceiver         receiver=new MyReceiver();
    protected Executor           pool;
    protected static final short NAKACK_ID=ClassConfigurator.getProtocolId(NAKACK2.class);
    protected final static int   NUM_MSGS=50;

    @BeforeMethod
    protected void setUp() throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        nak=new NAKACK2();

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            public Object down(Message msg) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new TimeScheduler3();}
        };

        transport.setId((short)100);

        nak.setDownProtocol(transport);

        receiver.init(a,b);
        nak.setUpProtocol(receiver);

        nak.start();

        List<Address> members=new ArrayList<>(2);
        members.add(a); members.add(b);
        View view=new View(a, 1, members);

        // set the local address
        nak.down(new Event(Event.SET_LOCAL_ADDRESS,a));

        // set a dummy digest
        View tmp_view=View.create(a, 1, a,b);
        MutableDigest digest=new MutableDigest(tmp_view.getMembersRaw());
        digest.set(a,0,0);
        digest.set(b,0,0);
        nak.down(new Event(Event.SET_DIGEST,digest));

        // set dummy view
        nak.down(new Event(Event.VIEW_CHANGE,view));

        nak.down(new Event(Event.BECOME_SERVER));

        pool=new ThreadPoolExecutor(1, 100, 1000, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
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

        Set<Integer> no_duplicates=new HashSet<>(seqnos);

        System.out.println("sending " + seqnos.size() + " msgs (including duplicates); size excluding duplicates=" +
                no_duplicates.size());

        // we need to add our own messages (nak is for A), or else they will get discarded by NAKACK.handleMessage()
        Table<Message> win=nak.getWindow(a);
        for(int i=1; i <= NUM_MSGS; i++)
            win.add(i, msg(a, i, i, true));

        for(int i: seqnos) {
            boolean oob=Util.tossWeightedCoin(0.5);
            pool.execute(new Sender(b, i, i, oob));
            pool.execute(new Sender(a, i, i, oob));
        }

        ConcurrentMap<Address, Collection<Message>> msgs=receiver.getMsgs();
        Collection<Message> c1_list=msgs.get(a);
        Collection<Message> c2_list=msgs.get(b);

        long end_time=System.currentTimeMillis() + 10000;
        while(System.currentTimeMillis() < end_time) {
            int size_c1=c1_list.size();
            int size_c2=c2_list.size();
            System.out.println("size A = " + size_c1 + ", size B=" + size_c2);
            if(size_c1 == NUM_MSGS && size_c2 == NUM_MSGS)
                break;
            Util.sleep(1000);
        }

        System.out.println("A received " + c1_list.size() + " messages (expected=" + NUM_MSGS + ")");
        System.out.println("B received " + c2_list.size() + " messages (expected=" + NUM_MSGS + ")");

        assert c1_list.size() == NUM_MSGS : "[A] expected " + NUM_MSGS + " messages, but got " + c1_list.size();
        assert c2_list.size() == NUM_MSGS : "[B] expected " + NUM_MSGS + " messages, but got " + c2_list.size();
    }

    private static List<Integer> generateRandomNumbers(int from, int to) {
        List<Integer> retval=new ArrayList<>(20);
        for(int i=from; i <= to; i++)
            retval.add(i);
        Collections.shuffle(retval);
        return retval;
    }


    private void send(Address sender, long seqno, int number, boolean oob) {
        assert sender != null;
        nak.up(msg(sender,seqno,number,oob));
    }


    private static Message msg(Address sender, long seqno, int number, boolean oob) {
        Message msg=new Message(null, number).src(sender);
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        if(seqno != -1)
            msg.putHeader(NAKACK_ID, NakAckHeader2.createMessageHeader(seqno));
        return msg;
    }


    static class MyReceiver extends Protocol {
        final ConcurrentMap<Address, Collection<Message>> msgs=new ConcurrentHashMap<>();

        public ConcurrentMap<Address, Collection<Message>> getMsgs() {
            return msgs;
        }
        public void init(Address ... mbrs) {
            for(Address mbr: mbrs) {
                msgs.putIfAbsent(mbr, new ConcurrentLinkedQueue<>());
            }
        }

        public Object up(Message msg) {
            Address sender=msg.getSrc();
            Collection<Message> list=msgs.get(sender);
            if(list == null) {
                list=new ConcurrentLinkedQueue<>();
                Collection<Message> tmp=msgs.putIfAbsent(sender, list);
                if(tmp != null)
                    list=tmp;
            }
            list.add(msg);
            return null;
        }

        public void up(MessageBatch batch) {
            Address sender=batch.sender();
            for(Message msg: batch) {
                Collection<Message> list=msgs.get(sender);
                if(list == null) {
                    list=new ConcurrentLinkedQueue<>();
                    Collection<Message> tmp=msgs.putIfAbsent(sender, list);
                    if(tmp != null)
                        list=tmp;
                }
                list.add(msg);
            }
        }
    }

    class Sender implements Runnable {
        final Address sender;
        final long    seqno;
        final int     number;
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