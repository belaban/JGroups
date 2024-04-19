package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
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
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class NAKACK_Delivery_Test {
    protected Protocol           nak;
    protected Address            a, b;
    protected MyReceiver         receiver=new MyReceiver();
    protected Executor           pool;
    protected static final short NAK2=ClassConfigurator.getProtocolId(NAKACK2.class),
                                 NAK4=ClassConfigurator.getProtocolId(NAKACK4.class);
    protected final static int   NUM_MSGS=50;

    @DataProvider
    static Object[][] create() {
        return new Object[][]{
          {new NAKACK2()},
          {new NAKACK4()}
        };
    }

    protected void setUp(Protocol prot) throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        nak=prot;
        receiver.clear();

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
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
        for(Protocol p=nak; p != null; p=p.getDownProtocol())
            p.setAddress(a);
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
    public void testSendingOfRandomMessages(Protocol prot) throws Exception {
        setUp(prot);
        List<Integer> seqnos=generateRandomNumbers(1, NUM_MSGS);
        seqnos.addAll(generateRandomNumbers(1, NUM_MSGS));
        seqnos.addAll(generateRandomNumbers(Math.min(15, NUM_MSGS / 2), NUM_MSGS / 2));
        seqnos.addAll(generateRandomNumbers(2, NUM_MSGS));
        seqnos.addAll(generateRandomNumbers(5, Math.max(5, NUM_MSGS - 10)));

        Set<Integer> no_duplicates=new HashSet<>(seqnos);

        System.out.println("sending " + seqnos.size() + " msgs (including duplicates); size excluding duplicates=" +
                no_duplicates.size());

        // we need to add our own messages (nak is for A), or else they will get discarded by NAKACK.handleMessage()
        if(prot instanceof NAKACK2) {
            Table<Message> win=((NAKACK2)nak).getWindow(a);
            for(int i=1; i <= NUM_MSGS; i++)
                win.add(i, msg(prot, a, i, i, true));
        }
        else {
            Buffer<Message> buf=((NAKACK4)nak).getBuf(a);
            for(int i=1; i <= NUM_MSGS; i++)
                buf.add(i, msg(prot, a, i, i, true));
        }

        for(int i: seqnos) {
            boolean oob=Util.tossWeightedCoin(0.5);
            pool.execute(new Sender(prot, b, i, i, oob));
            pool.execute(new Sender(prot, a, i, i, oob));
        }

        ConcurrentMap<Address,Collection<Message>> msgs=receiver.getMsgs();
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

    public void testBatchDeliveredWithTrace(Protocol prot) throws Exception {
        setUp(prot);
        doBatchDeliverTest(prot,true);
    }

    public void testBatchDeliveredWithoutTrace(Protocol prot) throws Exception {
        setUp(prot);
        doBatchDeliverTest(prot,false);
    }

    /**
     * Test for <a href="https://issues.redhat.com/browse/JGRP-2619">JGRP-2619</a>
     */
    private void doBatchDeliverTest(Protocol prot, boolean trace) {
        try {
            nak.setLevel("trace");

            // batch: first message without header, last message with header
            receiver.getMsgs().get(b).clear();
            assert receiver.getMsgs().get(b).isEmpty();

            MessageBatch batch = new MessageBatch(2).setMode(MessageBatch.Mode.OOB).setSender(b).setDest(null);
            batch.add(new EmptyMessage().setFlag(Message.Flag.NO_RELIABILITY, Message.Flag.OOB).src(b)); // no NAKACK2 header
            batch.add(msg(prot, b,1, 1, true));
            nak.up(batch);

            // expect both messages delivered
            System.out.println(receiver.getMsgs().get(b));
            assert receiver.getMsgs().get(b).size() == 2;

            // new batch, first message with header, last message without header
            receiver.getMsgs().get(b).clear();
            assert receiver.getMsgs().get(b).isEmpty();

            batch = new MessageBatch(2).setMode(MessageBatch.Mode.OOB).setSender(b).setDest(null);
            batch.add(msg(prot, b,2, 1, true));
            batch.add(new EmptyMessage().setFlag(Message.Flag.NO_RELIABILITY, Message.Flag.OOB).src(b)); // no NAKACK2 header
            nak.up(batch);

            // expect both messages delivered
            System.out.println(receiver.getMsgs().get(b));
            assert receiver.getMsgs().get(b).size() == 2;
        } finally {
            nak.setLevel("warn");
        }
    }

    private static List<Integer> generateRandomNumbers(int from, int to) {
        List<Integer> retval=new ArrayList<>(20);
        for(int i=from; i <= to; i++)
            retval.add(i);
        Collections.shuffle(retval);
        return retval;
    }


    private void send(Protocol prot, Address sender, long seqno, int number, boolean oob) {
        assert sender != null;
        nak.up(msg(prot, sender,seqno,number,oob));
    }


    private static Message msg(Protocol prot, Address sender, long seqno, int number, boolean oob) {
        Message msg=new BytesMessage(null, number).setSrc(sender);
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        if(seqno != -1) {
            if(prot instanceof NAKACK2)
                msg.putHeader(NAK2, NakAckHeader2.createMessageHeader(seqno));
            else
                msg.putHeader(NAK4, NakAckHeader.createMessageHeader(seqno));
        }
        return msg;
    }


    protected static class MyReceiver extends Protocol {
        final ConcurrentMap<Address, Collection<Message>> msgs=new ConcurrentHashMap<>();

        public ConcurrentMap<Address, Collection<Message>> getMsgs() {
            return msgs;
        }
        public MyReceiver clear() {msgs.clear(); return this;}
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
        final Protocol prot;

        public Sender(Protocol prot, Address sender, long seqno, int number, boolean oob) {
            this.sender=sender;
            this.seqno=seqno;
            this.number=number;
            this.oob=oob;
            this.prot=prot;
        }

        public void run() {
            send(prot, sender, seqno, number, oob);
        }
    }

}