package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.NAKACK3;
import org.jgroups.protocols.NakAckHeader;
import org.jgroups.util.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/** Functional tests of {@link Buffer} implementations ({@link DynamicBuffer} and {@link FixedBuffer}).
 * @author Bela Ban
 * @since 5.4
 */
@Test(groups=Global.FUNCTIONAL,dataProvider="windowCreator")
public class BufferTest {
    protected static final Predicate<Message>     dont_loopback_filter=m -> m != null && m.isFlagSet(DONT_LOOPBACK);
    protected static final short                  NAKACK3_ID;
    protected static final Function<Message,Long> SEQNO_GETTER;

    @DataProvider
    static Object[][] windowCreator() {
        return new Object[][]{
          // {new DynamicBuffer<>(0)},
          {new FixedBuffer<>(0)}
        };
    }

    static {
        NAKACK3_ID=ClassConfigurator.getProtocolId(NAKACK3.class);
        SEQNO_GETTER=m -> {
            NakAckHeader hdr=m.getHeader(NAKACK3_ID);
            return hdr == null? -1 : hdr.getSeqno();
        };
    }

    public void testCreation(Buffer<Integer> win) {
        System.out.println("win = " + win);
        int size=win.size();
        assert size == 0;
        assert win.get(15) == null;
        assertIndices(win, 0, 0, 0);
    }

    public void testAdd(Buffer<Integer> buf) {
        boolean rc=buf.add(1, 322649);
        assert rc;
        rc=buf.add(1, 100000);
        assert !rc;
        assert buf.size() == 1;
        rc=buf.add(2, 100000);
        assert rc;
        assert buf.size() == 2;
    }

    public void testAddList(Buffer<Integer> buf) {
        List<LongTuple<Integer>> msgs=createList(1, 2);
        boolean rc=add(buf, msgs);
        System.out.println("buf = " + buf);
        assert rc;
        assert buf.size() == 2;
    }

    public void testAddMessageBatch(Buffer<Message> buf) {
        MessageBatch mb=createMessageBatch(1, 2);
        boolean rc=buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        assert rc;
        assert buf.size() == 2;
    }

    public void testAddMessageBatchWithConstValue(Buffer<Message> buf) {
        MessageBatch mb=createMessageBatch(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final Message DUMMY=new EmptyMessage();
        boolean rc=buf.add(mb, SEQNO_GETTER, false, DUMMY);
        System.out.println("buf = " + buf);
        assert rc;
        assert buf.size() == 10;
        List<Message> list=buf.removeMany(true, 0, element -> element.hashCode() == DUMMY.hashCode());
        System.out.println("list = " + list);
        assert list.size() == 10;
        for(Message msg: list)
            assert msg == DUMMY;
    }

    public void testAddListWithRemoval(Buffer<Integer> buf) {
        List<LongTuple<Integer>> msgs=createList(1,2,3,4,5,6,7,8,9,10);
        boolean added=add(buf, msgs);
        System.out.println("buf = " + buf);
        assert added;

        msgs=createList(1,3,5,7);
        added=add(buf, msgs);
        System.out.println("buf = " + buf);
        assert !added;

        msgs=createList(1,3,5,7,9,10,11,12,13,14,15);
        added=add(buf, msgs);
        System.out.println("buf = " + buf);
        assert added;
    }

    public void testAddMessageBatchWithRemoval(Buffer<Message> buf) {
        MessageBatch mb=createMessageBatch(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        int size=mb.size();
        boolean added=buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        assert added;
        assert mb.size() == size;

        mb=createMessageBatch(1,3,5,7);
        size=mb.size();
        added=buf.add(mb, SEQNO_GETTER, true, null);
        System.out.println("buf = " + buf);
        assert !added;
        assert mb.isEmpty();

        mb=createMessageBatch(1,3,5,7,9,10,11,12,13,14,15);
        size=mb.size();
        added=buf.add(mb, SEQNO_GETTER, true, null);
        System.out.println("buf = " + buf);
        assert added;
        assert mb.isEmpty();
    }

    public void testAddMessageBatchWithFullBuffer(Buffer<Message> buf) {
        if(buf instanceof DynamicBuffer)
            return;
        buf=new FixedBuffer<>(10,0);
        addMessageBatchWithFullBuffer(buf, false);
    }

    public void testAddMessageBatchWithFullBufferOOB(Buffer<Message> buf) {
        if(buf instanceof DynamicBuffer)
            return;
        buf=new FixedBuffer<>(10,0);
        addMessageBatchWithFullBuffer(buf, true);
    }

    protected static void addMessageBatchWithFullBuffer(Buffer<Message> buf, boolean oob) {
        MessageBatch mb=createMessageBatch(1, 10, oob);
        int size=mb.size();
        boolean added=buf.add(mb, SEQNO_GETTER, !oob, null);
        System.out.println("buf = " + buf);
        assert added;
        assert mb.size() == (oob? size : 0);

        mb=createMessageBatch(11, 15, oob);
        size=mb.size();
        added=buf.add(mb, SEQNO_GETTER, !oob, null);
        System.out.println("buf = " + buf);
        assert !added;
        assert  mb.isEmpty();

        mb=createMessageBatch(1, 15, oob);
        size=mb.size();
        added=buf.add(mb, SEQNO_GETTER, !oob, null);
        System.out.println("buf = " + buf);
        assert !added;
        assert mb.isEmpty();

        buf.removeMany(true, 5);
        assert buf.size() == 5;
        mb=createMessageBatch(5, 15, oob);
        size=mb.size();
        added=buf.add(mb, SEQNO_GETTER, !oob, null);
        System.out.println("buf = " + buf);
        assert added;
        assert mb.size() == (oob? 5 : 0);
    }


    public void testAddition(Buffer<Integer> buf) {
        if(buf instanceof DynamicBuffer)
            buf=new DynamicBuffer<>(3, 10, 0);
        assert !buf.add(0, 0);
        addAndGet(buf,  1,5,9,10,11,19,20,29);
        System.out.println("buf: " + buf.dump());
        assert buf.size() == 8;
        int size=buf.computeSize();
        assert size == 8;
        assert buf.size() == buf.computeSize();
        if(buf instanceof DynamicBuffer)
            assertCapacity(buf.capacity(), 3, 10);
        assertIndices(buf, 0, 0, 29);
    }


    public void testAdditionList(Buffer<Integer> buf) {
        List<LongTuple<Integer>> msgs=createList(0);
        assert !add(buf, msgs);
        long[] seqnos={1,5,9,10,11,19,20,29};
        msgs=createList(seqnos);
        assert add(buf, msgs);
        System.out.println("buf: " + buf.dump());
        for(long seqno: seqnos)
            assert buf.get(seqno) == seqno;
        assert buf.size() == 8;
        int size=buf.computeSize();
        assert size == 8;
        assert buf.size() == buf.computeSize();
        assertIndices(buf, 0, 0, 29);
    }

    public void testAdditionMessageBatch(Buffer<Message> buf) {
        MessageBatch mb=createMessageBatch(0);
        assert !buf.add(mb, SEQNO_GETTER, false, null);
        long[] seqnos={1,5,9,10,11,19,20,29};
        mb=createMessageBatch(seqnos);
        assert buf.add(mb, SEQNO_GETTER, false, null);
        for(long seqno: seqnos) {
            Message m=buf.get(seqno);
            assert SEQNO_GETTER.apply(m) == seqno;
        }
        assert buf.size() == 8;
        int size=buf.computeSize();
        assert size == 8;
        assert buf.size() == buf.computeSize();
        assertIndices(buf, 0, 0, 29);
    }

    public void testAdditionWithOffset(Buffer<Integer> type) {
        final long offset=100;
        Buffer<Integer> buf=type instanceof DynamicBuffer?
          new DynamicBuffer<>(offset) : new FixedBuffer<>(32, offset);
        addAndGet(buf, 101,105,109,110,111,119,120,129);
        System.out.println("buf: " + buf.dump());
        assert buf.size() == 8;
        assertIndices(buf, 100, 100, 129);
    }

    public void testAdditionListWithOffset(Buffer<Integer> type) {
        final long offset=100;
        Buffer<Integer> buf=type instanceof DynamicBuffer?
          new DynamicBuffer<>(offset) : new FixedBuffer<>(32, offset);
        long[] seqnos={101,105,109,110,111,119,120,129};
        List<LongTuple<Integer>> msgs=createList(seqnos);
        System.out.println("buf: " + buf.dump());
        assert add(buf, msgs);
        assert buf.size() == 8;
        for(long seqno: seqnos)
            assert buf.get(seqno) == seqno;
        assertIndices(buf, 100, 100, 129);
    }

    public void testAdditionMessageBatchWithOffset(Buffer<Message> type) {
        final long offset=100;
        Buffer<Message> buf=type instanceof DynamicBuffer?
          new DynamicBuffer<>(offset) : new FixedBuffer<>(32, offset);
        long[] seqnos={101,105,109,110,111,119,120,129};
        MessageBatch mb=createMessageBatch(seqnos);
        System.out.println("buf: " + buf.dump());
        assert buf.add(mb, SEQNO_GETTER, false, null);
        assert buf.size() == 8;
        for(long seqno: seqnos)
            assert SEQNO_GETTER.apply(buf.get(seqno)) == seqno;
        assertIndices(buf, 100, 100, 129);
    }

    public void testAddBatchWithResizing(Buffer<Message> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Message> buf=new DynamicBuffer<>(3, 5, 0);
        MessageBatch mb=createMessageBatch(IntStream.rangeClosed(1, 100).asLongStream().toArray());
        buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        int num_resizes=buf.getNumResizes();
        System.out.println("num_resizes = " + num_resizes);
        assert num_resizes == 1 : "number of resizings=" + num_resizes + " (expected 1)";
    }

    public void testAddMessageBatchWithResizing(Buffer<Message> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Message> buf=new DynamicBuffer<>(3, 5, 0);
        long[] seqnos=LongStream.rangeClosed(1, 99).toArray();
        MessageBatch mb=createMessageBatch(seqnos);
        buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        int num_resizes=buf.getNumResizes();
        System.out.println("num_resizes = " + num_resizes);
        assert num_resizes == 1 : "number of resizings=" + num_resizes + " (expected 1)";
    }


    public void testAddBatchWithResizingNegativeSeqnos(Buffer<Message> type) {
        if(type instanceof FixedBuffer)
            return;
        long seqno=Long.MAX_VALUE-50;
        DynamicBuffer<Message> buf=new DynamicBuffer<>(3, 5, seqno);
        MessageBatch mb=createMessageBatch(IntStream.rangeClosed(1,100).asLongStream().toArray());
        buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        int num_resizes=buf.getNumResizes();
        System.out.println("num_resizes = " + num_resizes);
        assert num_resizes == 1 : "number of resizings=" + num_resizes + " (expected 1)";
    }

    public void testAddBatchWithResizing2(Buffer<Message> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Message> buf=new DynamicBuffer<>(3, 500, 0);
        MessageBatch mb=createMessageBatch(IntStream.rangeClosed(1,100).asLongStream().toArray());
        buf.add(mb, SEQNO_GETTER, false, null);
        System.out.println("buf = " + buf);
        int num_resizes=buf.getNumResizes();
        System.out.println("num_resizes = " + num_resizes);
        assert num_resizes == 0 : "number of resizings=" + num_resizes + " (expected 0)";
    }

    public void testAdditionWithOffset2(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 2)
          : new FixedBuffer<>(1029, 2);
        addAndGet(buf, 1000,1001);
        if(buf instanceof DynamicBuffer)
            ((DynamicBuffer<Integer>)buf).compact();
        addAndGet(buf, 1005, 1009, 1010, 1011, 1019, 1020, 1029);
        System.out.println("buf: " + buf.dump());
        assert buf.size() == 9;
        assertIndices(buf, 2, 2, 1029);
    }

    public void testAddWithWrapAround(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 5) :
          new FixedBuffer<>(5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        for(int i=0; i < 3; i++) {
            Integer val=buf.remove(false);
            System.out.println("removed " + val);
            assert val != null;
        }
        System.out.println("buf = " + buf);

        long low=buf.low();
        buf.purge(8);
        System.out.println("buf = " + buf);
        assert buf.low() == 8;
        for(long i=low; i <= 8; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        while(buf.remove(false) != null)
            ;
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        assert buf.numMissing() == 0;
        low=buf.low();
        buf.purge(18);
        assert buf.low() == 18;
        for(long i=low; i <= 18; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testAddWithWrapAroundAndRemoveMany(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 5) :
          new FixedBuffer<>(16, 5);
        for(int i=6; i <= 15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        List<Integer> removed=buf.removeMany(true, 3);
        System.out.println("removed " + removed);
        System.out.println("buf = " + buf);
        for(int i: removed)
            assert buf._get(i) == null;
        assertIndices(buf, 8, 8, 15);

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        removed=buf.removeMany(true, 0);
        System.out.println("buf = " + buf);
        System.out.println("removed = " + removed);
        assert removed.size() == 10;
        for(int i: removed)
            assert buf._get(i) == null;

        assert buf.isEmpty();
        assert buf.numMissing() == 0;
        assertIndices(buf, 18, 18, 18);
    }

    public void testAddAndWrapAround(Buffer<Integer> type) {
        long seqno=Long.MAX_VALUE-10;
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(seqno) : new FixedBuffer<>(16, seqno);
        for(int i=1; i <= 16; i++)
            buf.add(seqno+i,i);
        assert buf.size() == 16;
        List<Integer> list=buf.removeMany(true, 5);
        assert list.size() == 5;
        assert buf.size() == 11;
        assert list.equals(IntStream.rangeClosed(1,5).boxed().collect(Collectors.toList()));

        for(int i=17; i <= 21; i++)
            buf.add(seqno+i,i);
        assert buf.size() == 16;
        list.clear();
        buf.forEach((__,el) -> {
            list.add(el);
            return true;
        }, false);
        assert list.size() == 16;
        Iterator<Integer> it=list.iterator();
        for(int i=6; i <= 21; i++) {
            Integer val=it.next();
            assert i == val;
        }
        assert buf.size() == 16;
        buf.removeMany(true, 0);
        assert buf.isEmpty();
    }

    public void testAddMissing(Buffer<Integer> buf) {
        for(int i: Arrays.asList(1,2,4,5,6))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 5 && buf.numMissing() == 1;

        Integer num=buf.remove();
        assert num == 1;
        num=buf.remove();
        assert num == 2;
        num=buf.remove();
        assert num == null;

        buf.add(3, 3);
        System.out.println("buf = " + buf);
        assert buf.size() == 4 && buf.numMissing() == 0;

        for(int i=3; i <= 6; i++) {
            num=buf.remove();
            System.out.println("buf = " + buf);
            assert num == i;
        }
        num=buf.remove();
        assert num == null;
    }

    public void testDuplicateAddition(Buffer<Integer> buf) {
        addAndGet(buf, 1, 5, 9, 10);
        assert !buf.add(5,5);
        assert buf.get(5) == 5;
        assert buf.size() == 4;
        assertIndices(buf, 0, 0, 10);
    }

    public void testAddWithInvalidSeqno(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 20)
          : new FixedBuffer<>(20);
        boolean success=buf.add(10, 0);
        assert !success;
        success=buf.add(20, 0);
        assert !success;
        assert buf.isEmpty();
    }

    /** Runs NUM adders, each adds 1 unique seqno. When all adders are done, we should have NUM elements in the buf */
    public void testConcurrentAdd(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        final int NUM=100;
        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+1, buf);
            adders[i].start();
        }
        System.out.println("starting threads");
        latch.countDown();
        System.out.print("waiting for threads to be done: ");
        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        for(Adder adder: adders)
            assert adder.success();
        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == NUM;
    }

    public void testAddAndRemove(Buffer<Message> buf) {
        buf.add(1, msg(1));
        buf.add(2, msg(2));
        assert buf.getHighestDeliverable() == 2;
        buf.removeMany(true, 10);
        assert buf.highestDelivered() == 2;
        buf.add(3, msg(3));
        assert buf.high() == 3;

        buf.add(4, msg(4, true), dont_loopback_filter, true);
        assert buf.highestDelivered() == 2;
        assert buf.getHighestDeliverable() == 4;
        buf.removeMany(false, 10);
        assert buf.highestDelivered() == 4;

        buf.add(5, msg(5, true), dont_loopback_filter, true);
        buf.add(6, msg(6, true), dont_loopback_filter, true);
        assert buf.highestDelivered() == 6;
        assert IntStream.rangeClosed(1,2).allMatch(n -> buf._get(n) == null);
        assert IntStream.rangeClosed(3,6).allMatch(n -> buf._get(n) != null);
    }

    public void testAddAndRemove2(Buffer<Message> buf) {
        for(int i=1; i <=10; i++)
            buf.add(i, msg(i, true), dont_loopback_filter, true);
        assert buf.highestDelivered() == 10;
        assert buf.high() == 10;
        assert buf.getHighestDeliverable() == 10;
        buf.purge(10);
        assert buf.highestDelivered() == 10;
        assert buf.high() == 10;
        assert buf.getHighestDeliverable() == 10;
        assert buf.low() == 10;
    }


    public void testAddAndRemove3(Buffer<Message> type) {
        Buffer<Message> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 3)
          : new FixedBuffer<>(3);
        buf.add(5, msg(5, true), dont_loopback_filter, true);
        buf.add(6, msg(6, true), dont_loopback_filter, true);
        buf.add(4, msg(4, true), dont_loopback_filter, true);
        assert buf.high() == 6;
        assert buf.getHighestDeliverable() == 6;
        assert buf.highestDelivered() == 6;
        assert buf.isEmpty();
    }

    public void testAddAndRemove4(Buffer<Message> type) {
        Buffer<Message> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 3)
          : new FixedBuffer<>(3);
        buf.add(7, msg(7, true), dont_loopback_filter, true);
        buf.add(6, msg(6, true), dont_loopback_filter, true);
        buf.add(4, msg(4, true), dont_loopback_filter, true);
        assert buf.high() == 7;
        assert buf.getHighestDeliverable() == 4;
        assert buf.highestDelivered() == 4;
    }

    public void testNonBlockingAdd(Buffer<Integer> buf) {
        if(buf instanceof DynamicBuffer)
            return;
        buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= 10; i++)
            assert buf.add(i, i);
        boolean rc=buf.add(11, 11, null, false);
        assert !rc;
        assert buf.size() == 10;
    }

    public void testAddListWithConstValue(Buffer<Integer> buf) {
        List<LongTuple<Integer>> msgs=createList(1,2,3,4,5,6,7,8,9,10);
        final int DUMMY=0;
        boolean rc=buf.add(msgs, false, DUMMY);
        System.out.println("buf = " + buf);
        assert rc;
        assert buf.size() == 10;
        List<Integer> list=buf.removeMany(true, 0, element -> element.hashCode() == Integer.hashCode(DUMMY));
        System.out.println("list = " + list);
        assert list.size() == 10;
        assert buf.isEmpty();
        for(int num: list)
            assert num == DUMMY;
    }

    public void testAddListWithResizingNegativeSeqnos(Buffer<Integer> type) {
        long seqno=Long.MAX_VALUE-50;
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3,5,seqno) : new FixedBuffer<>(100, seqno);
        List<LongTuple<Integer>> msgs=new ArrayList<>();
        for(int i=1; i < 100; i++)
            msgs.add(new LongTuple<>((long)i+seqno,i));
        buf.add(msgs, false, null);
        System.out.println("buf = " + buf);
        if(type instanceof DynamicBuffer) {
            int num_resizes=((DynamicBuffer<?>)buf).getNumResizes();
            System.out.println("num_resizes = " + num_resizes);
            assert num_resizes == 1 : "number of resizings=" + num_resizes + " (expected 1)";
        }
    }

    public void testAddListWithRemoval2(Buffer<Integer> buf) {
        List<LongTuple<Integer>> msgs=createList(1,2,3,4,5,6,7,8,9,10);
        int size=msgs.size();
        boolean added=buf.add(msgs, false, null);
        System.out.println("buf = " + buf);
        assert added;
        assert msgs.size() == size;

        added=buf.add(msgs, true, null);
        System.out.println("buf = " + buf);
        assert !added;
        assert msgs.isEmpty();

        msgs=createList(1,3,5,7);
        size=msgs.size();
        added=buf.add(msgs, true, null);
        System.out.println("buf = " + buf);
        assert !added;
        assert msgs.isEmpty();

        msgs=createList(1,3,5,7,9,10,11,12,13,14,15);
        size=msgs.size();
        added=buf.add(msgs, true, null);
        System.out.println("buf = " + buf);
        assert added;
        assert msgs.size() == 5;
    }

    public void testAddListWithResizing2(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>() : new FixedBuffer<>(100, 0);
        List<LongTuple<Integer>> msgs=new ArrayList<>();
        for(int i=1; i < 100; i++)
            msgs.add(new LongTuple<>(i, i));
        buf.add(msgs, false, null);
        System.out.println("buf = " + buf);
        if(buf instanceof DynamicBuffer) {
            int num_resizes=((DynamicBuffer<?>)buf).getNumResizes();
            System.out.println("num_resizes = " + num_resizes);
            assert num_resizes == 0 : "number of resizings=" + num_resizes + " (expected 0)";
        }
    }

    public void testAddListWithResizing(Buffer<Message> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3,5,0) : new FixedBuffer<>(100, 0);
        List<LongTuple<Integer>> msgs=new ArrayList<>();
        for(int i=1; i < 100; i++)
            msgs.add(new LongTuple<>(i, i));
        buf.add(msgs, false, null);
        System.out.println("buf = " + buf);
        if(buf instanceof DynamicBuffer) {
            int num_resizes=((DynamicBuffer<?>)buf).getNumResizes();
            System.out.println("num_resizes = " + num_resizes);
            assert num_resizes == 1 : "number of resizings=" + num_resizes + " (expected 1)";
        }
    }

    public void testIndex(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 5)
          : new FixedBuffer<>(10, 5);
        assertIndices(buf, 5,5,5);
        buf.add(6,6); buf.add(7,7);
        buf.remove(false); buf.remove(false);
        long low=buf.low();
        assert low == 5;
        assert buf.purge(4) == 0;
        assert buf.purge(5) == 0;
        assert buf.purge(6) == 1;
        assert buf.purge(7) == 1;
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testIndexWithRemoveMany(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 5)
          : new FixedBuffer<>(10, 5);
        assertIndices(buf, 5, 5, 5);
        buf.add(6, 6); buf.add(7, 7);
        long low=buf.low();
        buf.removeMany(true, 0);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        assertIndices(buf, 7, 7, 7);
    }

    public void testComputeSize(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        assert buf.computeSize() == 10;
        buf.removeMany(false, 3);
        System.out.println("buf = " + buf);
        assert buf.computeSize() == 7;

        buf.removeMany(true, 4);
        System.out.println("buf = " + buf);
        assert buf.computeSize() == buf.size();
        assert buf.computeSize() == 3;
    }

    public void testComputeSize2(Buffer<Integer> buf) {
        buf.add(1, 1);
        System.out.println("buf = " + buf);
        assert buf.computeSize() == buf.size();
        assert buf.computeSize() == 1;
        buf.remove(false);
        System.out.println("buf = " + buf);
        assert buf.computeSize() == buf.size();
        assert buf.computeSize() == 0;
    }

    public void testRemove(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,9).boxed().forEach(n -> buf.add(n,n));
        buf.add(20, 20);
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
        assertIndices(buf, 0, 0, 20);

        int num_null_msgs=buf.numMissing();
        assert num_null_msgs == 10;

        for(long i=1; i <= 10; i++) // 10 is missing
            buf.remove();
        System.out.println("buf = " + buf);
        assert buf.size() == 1;
        assertIndices(buf, 9, 9, 20);

        num_null_msgs=buf.numMissing();
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;
    }

    public void testRemove2(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,5).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 5);

        Integer el=buf.remove(true);
        System.out.println("el = " + el);
        assert el.equals(1);

        el=buf.remove(false);  // not encouraged ! nullify should always be true or false
        System.out.println("el = " + el);
        assert el.equals(2);

        el=buf.remove(true);
        System.out.println("el = " + el);
        assert el.equals(3);
    }

    public void testRemove3(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,5).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 5);

        for(int i=1; i <= 5; i++) {
            Integer el=buf.remove();
            System.out.println("el = " + el);
            assert el.equals(i);
        }
        assert buf.isEmpty();
    }

    public void testRemove4(Buffer<Integer> buf) {
        Integer el=buf.remove();
        assert el == null; // low == high
        assert buf.low() == 0;
        buf.add(2,2);
        el=buf.remove();
        assert el == null; // no element at 'low'
        assert buf.low() == 0;
        buf.add(1,1);
        el=buf.remove();
        assert el == 1;
        assert buf.low() == 1;
        el=buf.remove();
        assert el == 2;
        assert buf.low() == 2;
        el=buf.remove();
        assert el == null;
        assert buf.low() == 2;
    }

    public void testRemove5(Buffer<Integer> buf) {
        for(int i=1; i <= 20; i++) {
            assert buf.add(i,i);
            Integer num=buf.remove();
            assert num != null && num == i;
        }
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        assert buf.numMissing() == 0;
        assert buf.high() == 20;
        assert buf.low() == 20;
    }

    public void testRemoveMany(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,10).filter(n -> n != 6).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 10);
        List<Integer> list=buf.removeMany(true, 4);
        System.out.println("list=" + list + ", buf=" + buf);
        assert buf.size() == 5 && buf.numMissing() == 1;
        assert list != null && list.size() == 4;
        assert list.equals(List.of(1,2,3,4));
        assertIndices(buf, 4, 4, 10);
    }

    public void testRemoveMany2(Buffer<Integer> buf) {
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        List<Integer> list=buf.removeMany(false,3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;

        list=buf.removeMany(false, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;

        list=buf.removeMany(false, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(false, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
    }

    public void testRemoveManyWithNulling(Buffer<Integer> buf) {
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        assert list.equals(IntStream.rangeClosed(1,3).boxed().collect(Collectors.toList()));
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;
        assert list.equals(List.of(4,5,6,7));
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(false, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        assert list.equals(List.of(8,9,10));
        for(int i: list)
            assert buf._get(i) == null;
    }


    public void testRemoveManyWithWrapping(Buffer<Integer> buf) {
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            buf.add(seqno, seqno);
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 20);
        assert buf.size() == 18 && buf.numMissing() == 2;
        List<Integer> list=buf.removeMany(true, 0);
        assert list.size() == 12;
        assertIndices(buf, 12, 12, 20);
        assert buf.size() == 6 && buf.numMissing() == 2;
        buf.purge(12);
        assertIndices(buf, 12, 12, 20);
        assert buf.size() == 6 && buf.numMissing() == 2;
    }

    public void testRemoveManyWithWrapping2(Buffer<Integer> buf) {
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            buf.add(seqno, seqno);
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 20);
        assert buf.size() == 18 && buf.numMissing() == 2;
        List<Integer> list=buf.removeMany(false,0);
        assert list.size() == 12;
        assertIndices(buf, 0, 12, 20);
        assert buf.size() == 6 && buf.numMissing() == 2;
        buf.purge(12);
        assertIndices(buf, 12, 12, 20);
        assert buf.size() == 6 && buf.numMissing() == 2;
    }

    public void testRemoveManyWithFilter(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 0, element -> element % 2 == 0);
        System.out.println("list = " + list);
        System.out.println("buf = " + buf);
        assert list.size() == 5;
        assert buf.isEmpty();
        for(Integer num: Arrays.asList(2,4,6,8,10))
            assert list.contains(num);
    }

    public void testRemoveManyWithFilterAcceptAll(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 0, element -> true);
        System.out.println("list = " + list);
        System.out.println("buf = " + buf);
        assert list.size() == 10;
        assert buf.isEmpty();
    }

    public void testRemoveManyWithFilterAcceptNone(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 0, element -> false);
        System.out.println("list = " + list);
        System.out.println("buf = " + buf);
        assert list == null;
        assert buf.isEmpty();
    }

    public void testRemoveManyWithFilterAcceptNone2(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 3, new Predicate<>() {
            int cnt=0;
            public boolean test(Integer element) {
                return ++cnt <= 2;
            }
        });
        System.out.println("list = " + list);
        System.out.println("buf = " + buf);
        assert list.size() == 2;
        assert buf.isEmpty();
    }


    public void testRemoveMany3(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, i);
        List<Integer> result=buf.removeMany(true, 0, null, ArrayList::new, ArrayList::add);
        assert result != null && result.size() == 10;
        assert buf.isEmpty();
    }

    public void testRemoveManyIntoMessageBatch(Buffer<Message> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i, new BytesMessage(null, "hello"));

        MessageBatch batch=new MessageBatch(buf.size());
        Supplier<MessageBatch> batch_creator=() -> batch;
        BiConsumer<MessageBatch,Message> accumulator=MessageBatch::add;

        MessageBatch result=buf.removeMany(true, 0, null, batch_creator, accumulator);
        assert !batch.isEmpty();
        assert buf.isEmpty();
        assert batch.size() == 10;
        assert result != null && result == batch;

        IntStream.rangeClosed(11,15).forEach(seqno -> buf.add(seqno, new BytesMessage(null, "test")));

        batch.reset();
        result=buf.removeMany(true, 0, null, batch_creator, accumulator);
        assert !batch.isEmpty();
        assert buf.isEmpty();
        assert batch.size() == 5;
        assert result != null && result == batch;


        result=buf.removeMany( true, 0, null, batch_creator, accumulator);
        assert result == null;
    }

    public void testRemoveManyWithMaxBatchSize(Buffer<Message> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(1024, 0);
        final Buffer<Message> b=buf;
        IntStream.rangeClosed(1, 1024).forEach(n -> b.add(n,new ObjectMessage(null, "hello-" + n)));
        assert buf.size() == 1024;
        assert buf.numMissing() == 0;
        MessageBatch batch=new MessageBatch(128);
        Supplier<MessageBatch> batch_creator=() -> batch;
        BiConsumer<MessageBatch,Message> accumulator=MessageBatch::add;
        buf.removeMany(true, 512, null, batch_creator, accumulator);
        assert buf.size() == 512;
    }

    public void testForEach(Buffer<Integer> buf) {
        class MyVisitor implements Buffer.Visitor<Integer> {
            final List<Integer> list=new ArrayList<>(20);
            public boolean visit(long seqno, Integer element) {
                System.out.printf("#%d: %s\n", seqno, element);
                list.add(element);
                return true;
            }
        }
        MyVisitor visitor=new MyVisitor();
        for(int i=1; i <=20; i++)
           buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.forEach(visitor, false);
        List<Integer> list=visitor.list;
        assert list.size() == 20;
        List<Integer> tmp=IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());
        assert tmp.equals(list);
    }

    public void testIteration(Buffer<Integer> buf) {
        List<Integer> list=new ArrayList<>(20);
        IntStream.rangeClosed(1, 20).forEach(i -> {list.add(i); buf.add(i,i);});
        List<Integer> list2=new ArrayList<>();
        for(Integer i: buf)
            list2.add(i);
        System.out.println("list  = " + list);
        System.out.println("list2 = " + list2);
        assert list2.equals(list);
    }

    public void testStream(Buffer<Integer> buf) {
        List<Integer> list=new ArrayList<>(20);
        IntStream.rangeClosed(1, 20).forEach(i -> {list.add(i); buf.add(i,i);});
        List<Integer> list2=buf.stream().collect(ArrayList::new, ArrayList::add, (l,el) -> {});
        System.out.println("list  = " + list);
        System.out.println("list2 = " + list2);
        assert list2.equals(list);
    }

    public void testGet(Buffer<Integer> buf) {
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        assert buf.get(0) == null;
        assert buf.get(1) == 1;
        assert buf.get(10) == null;
        assert buf.get(5) == 5;
        assert buf.get(6) == null;
    }

    public void testGetNullMessages(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        buf.add(1, 1);
        buf.add(100, 100);
        System.out.println("buf = " + buf);
        int num_null_elements=buf.numMissing();
        assert num_null_elements == 98; // [2 .. 99]

        buf.add(50,50);
        System.out.println("buf = " + buf);
        assert buf.size() == 3;
        assert buf.numMissing() == 97;
    }

    public void testGetNullMessages2(Buffer<Integer> buf) {
        buf.add(1, 1);
        buf.add(5, 5);
        System.out.println("buf = " + buf);
        int num_null_elements=buf.numMissing();
        assert num_null_elements == 3; // [2 .. 4]

        buf.add(10,10);
        System.out.println("buf = " + buf);
        assert buf.size() == 3;
        assert buf.numMissing() == 7;

        buf.add(14,14);
        System.out.println("buf = " + buf);
        assert buf.size() == 4;
        assert buf.numMissing() == 10;

        while(buf.remove() != null)
            ;
        System.out.println("buf = " + buf);
        assert buf.size() == 3;
        assert buf.numMissing() == 10;
    }

    public void testGetMissing(Buffer<Integer> buf) {
        SeqnoList missing=buf.getMissing();
        assert missing == null;
        for(int num: Arrays.asList(2,4,6,8))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 4;
        assert buf.numMissing() == 4;
    }

    public void testGetMissing2(Buffer<Integer> buf) {
        for(int i: Arrays.asList(2,5,10,11,12,13,15,20,28,30))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int missing=buf.numMissing();
        System.out.println("missing=" + missing);
        SeqnoList missing_list=buf.getMissing();
        System.out.println("missing_list = " + missing_list);
        assert missing_list.size() == missing;
    }

    public void testGetMissing3(Buffer<Integer> buf) {
        for(int num: Arrays.asList(3,4,5))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 2; // the range [1-2]
        assert buf.numMissing() == 2;
    }

    public void testGetMissing4(Buffer<Integer> buf) {
        buf.add(1,1);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing == null && buf.numMissing() == 0;

        buf=new FixedBuffer<>(10, 0);
        buf.add(10,10);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new DynamicBuffer<>(0);
        buf.add(10,10);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new FixedBuffer<>(10, 0);
        buf.add(5,5);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new DynamicBuffer<>(0);
        buf.add(5,5);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();

        buf=new FixedBuffer<>(10, 0);
        buf.add(5,7);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.numMissing() == missing.size();
        buf=new DynamicBuffer<>(0);
        buf.add(5,5); buf.add(7,7);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing.size() == 5;
        assert buf.numMissing() == missing.size();
    }

    public void testGetMissing5(Buffer<Integer> buf) {
        buf.add(8, 8);
        System.out.println("buf = " + buf);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 7;
        assert buf.numMissing() == 7;
    }

    public void testGetMissingWithOffset(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, 300000)
          : new FixedBuffer<>(300000);

        SeqnoList missing=buf.getMissing();
        assert missing == null;

        for(int num: Arrays.asList(300002,300004,300006,300008))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 4;
        assert buf.numMissing() == 4;

        buf.add(300001,300001);
        buf.removeMany(true, 2);
        missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 3;
        assert buf.numMissing() == 3;
    }


    public void testGetMissingWithMaxSize(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(50, 0);
        for(int i=1; i <= 50; i++) {
            if(i % 2 == 0)
                buf.add(i,i);
        }
        assert buf.numMissing() == 25;
        SeqnoList missing=buf.getMissing();
        assert missing.size() == 25;

        missing=buf.getMissing(10);
        assert missing.size() == 5;

        missing=buf.getMissing(200);
        assert missing.size() == 25;
    }

    public void testGetMissingWithMaxBundleSize(Buffer<Integer> buf) {
        final int max_bundle_size=64000, missing_msgs=1_000_000;
        final int max_xmit_req_size=(max_bundle_size -50) * Global.LONG_SIZE;
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(missing_msgs, 0);
        buf.add(0, 0);
        buf.add(missing_msgs, missing_msgs);
        System.out.println("buf = " + buf);

        SeqnoList missing=buf.getMissing(max_xmit_req_size);
        System.out.println("missing = " + missing);

        int serialized_size=missing.serializedSize();
        assert serialized_size <= max_bundle_size :
          String.format("serialized size of %d needs to be less than max_bundle_size of %d bytes", serialized_size, max_bundle_size);
    }

    public void testGetMissingWithMaxBundleSize2(Buffer<Integer> buf) {
        final int max_bundle_size=64000, missing_msgs=2_000_000;
        final int max_xmit_req_size=(max_bundle_size -50) * Global.LONG_SIZE;
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(missing_msgs, 0);
        for(int i=0; i < missing_msgs/2; i++)
            buf.add(i, i);

        buf.add(missing_msgs, missing_msgs);
        System.out.println("buf = " + buf);

        SeqnoList missing=buf.getMissing(max_xmit_req_size);
        System.out.println("missing = " + missing);

        int serialized_size=missing.serializedSize();
        assert serialized_size <= max_bundle_size :
          String.format("serialized size of %d needs to be less than max_bundle_size of %d bytes", serialized_size, max_bundle_size);

        int limit=missing_msgs/2 + max_xmit_req_size;
        for(long l: missing) {
            assert l >= missing_msgs/2 && l < limit;
        }
    }

    public static void testGetMissingLast(Buffer<Integer> buf) {
        for(int num: Arrays.asList(1,2,3,4,5,6,8))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 1;
        assert buf.numMissing() == 1;
    }

    public static void testGetMissingFirst(Buffer<Integer> buf) {
        for(int num: Arrays.asList(2,3,4,5))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 1;
        assert buf.numMissing() == 1;
    }

    public void testGetHighestDeliverable(Buffer<Integer> buf) {
        System.out.println("buf = " + buf);
        long highest_deliverable=buf.getHighestDeliverable(), hd=buf.highestDelivered();
        int num_deliverable=buf.getNumDeliverable();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 0;
        assert highest_deliverable == 0;
        assert num_deliverable == 0;

        for(int num: Arrays.asList(1,2,3,4,5,6,8))
            buf.add(num, num);
        System.out.println("buf = " + buf);
        highest_deliverable=buf.getHighestDeliverable();
        num_deliverable=buf.getNumDeliverable();
        hd=buf.highestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 0;
        assert highest_deliverable == 6;
        assert num_deliverable == 6;

        buf.removeMany(true, 4);
        System.out.println("buf = " + buf);
        highest_deliverable=buf.getHighestDeliverable();
        num_deliverable=buf.getNumDeliverable();
        hd=buf.highestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 4;
        assert highest_deliverable == 6;
        assert num_deliverable == 2;

        buf.removeMany(true, 100);
        System.out.println("buf = " + buf);
        highest_deliverable=buf.getHighestDeliverable();
        hd=buf.highestDelivered();
        num_deliverable=buf.getNumDeliverable();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 6;
        assert highest_deliverable == 6;
        assert num_deliverable == 0;

        buf.add(7,7);
        System.out.println("buf = " + buf);
        highest_deliverable=buf.getHighestDeliverable();
        num_deliverable=buf.getNumDeliverable();
        hd=buf.highestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 6;
        assert highest_deliverable == 8;
        assert num_deliverable == 2;

        buf.removeMany(true, 100);
        System.out.println("buf = " + buf);
        highest_deliverable=buf.getHighestDeliverable();
        num_deliverable=buf.getNumDeliverable();
        hd=buf.highestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 8;
        assert highest_deliverable == 8;
        assert num_deliverable == 0;
    }

    public void testGetHighestDeliverable2(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i,i);
        System.out.println("buf = " + buf);
        buf.removeMany(true, 20);
        long highest_deliverable=buf.getHighestDeliverable();
        assert highest_deliverable == 10;
        assert buf.highestDelivered() == highest_deliverable;
    }

    public void testGetHighestDeliverable3(Buffer<Integer> buf) {
        for(int i=1; i <= 10; i++)
            buf.add(i,i);
        System.out.println("buf = " + buf);
        buf.removeMany(true, 9);
        long highest_deliverable=buf.getHighestDeliverable();
        assert highest_deliverable == 10;
    }

    public void testMassAddition(Buffer<Integer> type) {
        final int NUM_ELEMENTS=10005;
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3,10,0) : new FixedBuffer<>(NUM_ELEMENTS, 0);
        for(int i=1; i <= NUM_ELEMENTS; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == NUM_ELEMENTS;
        if(buf instanceof DynamicBuffer)
            assertCapacity(buf.capacity(), ((DynamicBuffer<Integer>)buf).getNumRows(), 10);
        else
            assert buf.capacity() == NUM_ELEMENTS;
        assertIndices(buf, 0, 0, NUM_ELEMENTS);
        assert buf.numMissing() == 0;
    }

    public void testResize(Buffer<Integer> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Integer> buf=new DynamicBuffer<>(3, 10, 0);
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);
        addAndGet(buf, 30);
        addAndGet(buf,35);
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);
        addAndGet(buf,500);
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);

        addAndGet(buf, 515);
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);
    }

    public void testResizeWithPurge(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        for(int i=1; i <= 100; i++)
            addAndGet(buf, i);
        System.out.println("buf: " + buf);
        
        // now remove 60 messages
        for(int i=1; i <= 60; i++) {
            Integer num=buf.remove();
            assert num != null && num == i;
        }
        System.out.println("buf after removal of seqno 60: " + buf);

        buf.purge(50);
        System.out.println("now triggering a resize() by addition of seqno=120 (not applicable to FixedBuffer)");
        addAndGet(buf, 120);
    }

    public void testResizeWithPurgeAndGetOfNonExistingElement(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(50, 0);
        for(int i=1; i <= 50; i++)
            addAndGet(buf, i);
        System.out.println("buf: " + buf);
        assertIndices(buf, 0, 0, 50);
        assert buf.size() == 50 && buf.numMissing() == 0;

        // now remove 15 messages
        for(long i=1; i <= 15; i++) {
            Integer num=buf.remove(false);
            assert num != null && num == i;
        }
        System.out.println("buf after removal of seqno 15: " + buf);
        assertIndices(buf, 0, 15, 50);
        assert buf.size() == 35 && buf.numMissing() == 0;

        buf.purge(15);
        System.out.println("now triggering a resize() by addition of seqno=55");
        addAndGet(buf, 55);
        assertIndices(buf, 15, 15, 55);
        assert buf.size() == 36 && buf.numMissing() == 4;

        // now we have elements 40-49 in row 1 and 55 in row 2:
        List<Integer> list=new ArrayList<>(20);
        for(int i=16; i < 50; i++)
            list.add(i);
        list.add(55);

        for(long i=buf.offset(); i < buf.capacity() + buf.offset(); i++) {
            Integer num=buf._get(i);
            if(num != null) {
                System.out.println("num=" + num);
                list.remove(num);
            }
        }
        System.out.println("buf:\n" + buf.dump());
        assert list.isEmpty() : " list: " + Util.print(list);
    }

    public void testResizeWithPurge2(Buffer<Integer> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Integer> buf=new DynamicBuffer<>(3, 10, 0);
        for(int i=1; i <= 50; i++)
            addAndGet(buf, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 50;
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);
        assertIndices(buf, 0, 0, 50);
        buf.removeMany(false, 43);
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 43, 50);
        buf.purge(43);
        System.out.println("buf = " + buf);
        assertIndices(buf,43,43,50);
        addAndGet(buf, 52);
        assert buf.get(43) == null;

        for(long i=44; i <= 50; i++) {
            Integer num=buf.get(i);
            assert num != null && num == i;
        }

        assert buf.get(50) != null;
        assert buf.get(51) == null;
        Integer num=buf.get(52);
        assert num != null && num == 52;
        assert buf.get(53) == null;
    }


    public void testMove(Buffer<Integer> type) {
        if(type instanceof FixedBuffer)
            return;
        DynamicBuffer<Integer> buf=new DynamicBuffer<>(3, 10, 0);
        for(int i=1; i < 50; i++)
            addAndGet(buf, i);
        buf.removeMany(true, 49);
        assert buf.isEmpty();
        addAndGet(buf, 50);
        assert buf.size() == 1;
        assertCapacity(buf.capacity(), buf.getNumRows(), 10);
    }

    public void testMove2(Buffer<Integer> buf) {
        for(int i=1; i < 30; i++)
            buf.add(i, i);
        buf.removeMany(true, 23);
        System.out.println("buf = " + buf);
        buf.add(35, 35); // triggers a resize() --> move()
        for(int i=1; i <= 23; i++)
            assert buf.get(i) == null;
        for(int i=24; i < 30; i++)
            assert buf.get(i) != null;
    }

    public void testMove3(Buffer<Integer> buf) {
        for(int i=1; i < 30; i++)
            buf.add(i, i);
        buf.removeMany(true, 23);
        System.out.println("buf = " + buf);
        buf.add(30, 30); // triggers a resize() --> move()
        for(int i=1; i <= 23; i++)
            assert buf._get(i) == null;
        for(int i=24; i < 30; i++)
            assert buf._get(i) != null;
    }


    public void testPurge(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(50, 0);
        for(int seqno=1; seqno <= 25; seqno++)
            buf.add(seqno, seqno);

        int[] seqnos={30,31,32,37,38,39,40,41,42,47,48,49};
        for(int seqno: seqnos)
            buf.add(seqno, seqno);

        System.out.println("buf (before remove):\n" + buf.dump());
        for(int seqno=1; seqno <= 22; seqno++)
            buf.remove(false);

        System.out.println("\nbuf (after remove 22, before purge):\n" + buf.dump());
        buf.purge(22);
        System.out.println("\nbuf: (after purge 22):\n" + buf.dump());
        assert buf.size() == 3 + seqnos.length;
        assert buf.computeSize() == buf.size();
    }

    public void testPurge2(Buffer<Integer> buf) {
        int purged=buf.purge(0);
        assert purged == 0;
        purged=buf.purge(1);
        assert purged == 0;
        purged=buf.purge(5);
        assert purged == 0;
        purged=buf.purge(32);
        assert purged == 0;
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        assert buf.size() == 10;
        purged=buf.purge(5, true);
        assert purged == 5 && buf.size() == 5;
    }

    public void testPurge3(Buffer<Integer> buf) {
        for(int i=1; i <=7; i++) {
            buf.add(i, i);
            buf.remove(false);
        }
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        long low=buf.low();
        if(buf instanceof DynamicBuffer) {
            buf.purge(3);
            assert buf.low() == 3;
            for(long i=low; i <= 3; i++)
                assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        }

        buf.purge(6);
        assert buf._get(6) == null;
        buf.purge(7);
        assert buf._get(7) == null;
        assert buf.low() == 7;
        assert buf.isEmpty();

        for(int i=7; i <= 14; i++) {
            buf.add(i, i);
            buf.remove(false);
        }

        System.out.println("buf = " + buf);
        assert buf.isEmpty();

        low=buf.low();
        assert !(buf instanceof DynamicBuffer) || low == 7;
        buf.purge(12);
        System.out.println("buf = " + buf);
        if(buf instanceof DynamicBuffer) {
            assert buf.low() == 12;
            for(long i=low; i <= 12; i++)
                assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        }
    }

    public void testPurge4(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        for(int i=1; i <= 100; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.removeMany(true, 53);
        for(int i=54; i <= 100; i++)
            assert buf.get(i) == i;
    }

    public void testPurge5(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        for(int i=1; i <= 100; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.removeMany(false, 53);
        buf.purge(53);
        for(int i=54; i <= 100; i++)
            assert buf.get(i) == i;
    }

    public void testPurge6(Buffer<Integer> buf) {
        if(buf instanceof FixedBuffer)
            buf=new FixedBuffer<>(100, 0);
        for(int i=1; i <= 100; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.removeMany(false, 0);

        buf.purge(10);
        for(int i=1; i <= 10; i++)
            assert buf._get(i) == null;
        if(buf instanceof DynamicBuffer) {
            for(int i=11; i <= 100; i++)
                assert buf.get(i) == i;
        }

        buf.purge(10);
        if(buf instanceof DynamicBuffer) {
            for(int i=11; i <= 100; i++)
                assert buf.get(i) == i;
        }

        buf.purge(50);
        for(int i=1; i <= 50; i++)
            assert buf._get(i) == null;
        if(buf instanceof DynamicBuffer) {
            for(int i=51; i <= 100; i++)
                assert buf.get(i) == i;
        }

        buf.purge(100);
        for(int i=51; i <= 100; i++)
            assert buf._get(i) == null;
    }


    public void testPurgeForce(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,30).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        buf.purge(15, true);
        System.out.println("buf = " + buf);
        assertIndices(buf, 15, 15, 30);
        for(int i=1; i <= 15; i++)
            assert buf._get(i) == null;
        for(int i=16; i<= 30; i++)
            assert buf._get(i) != null;
        assert buf.get(5) == null && buf.get(25) != null;

        buf.purge(30, true);
        System.out.println("buf = " + buf);
        assertIndices(buf, 30, 30, 30);
        assert buf.isEmpty();
        for(int i=1; i <= 30; i++)
            assert buf._get(i) == null;

        for(int i=31; i <= 40; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
        assertIndices(buf, 30, 30, 40);

        buf.purge(50, true);
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        assertIndices(buf, 40, 40, 40);
    }

    public void testPurgeForceWithGaps(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,30).filter(n -> n % 2 == 0).forEach(n -> buf.add(n,n));
        System.out.println("buf = " + buf);
        int purged=buf.purge(15, true);
        assert purged == 7;
        System.out.println("buf = " + buf);
        assertIndices(buf, 15, 15, 30);
        for(int i=1; i <= 15; i++)
            assert buf._get(i) == null;
        for(int i=16; i<= 30; i++) {
            if(i % 2 == 0)
                assert buf._get(i) != null;
            else
                assert buf._get(i) == null;
        }
        assert buf.get(5) == null && buf.get(26) != null;

        purged=buf.purge(30, true);
        assert purged == 8;
        System.out.println("buf = " + buf);
        assertIndices(buf, 30, 30, 30);
        assert buf.isEmpty();
    }

    // Tests purge(40) followed by purge(20) - the second purge() should be ignored
    // https://issues.redhat.com/browse/JGRP-1872
    public void testPurgeLower(Buffer<Integer> buf) {
        for(int i=1; i <= 30; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.purge(20, true);
        assertIndices(buf, 20, 20, 30);

        buf.purge(15, true);
        assertIndices(buf, 20,20, 30);

        buf=new DynamicBuffer<>(0);
        for(int i=1; i <= 30; i++)
            buf.add(i, i);
        System.out.println("buf = " + buf);
        buf.purge(20, true);
        assertIndices(buf, 20, 20, 30);

        buf.purge(15, false);
        assertIndices(buf, 20, 20, 30);
    }


    public void testCompact(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3,10,0) : new FixedBuffer<>(80, 0);
        IntStream.rangeClosed(1,80).boxed().forEach(n -> buf.add(n,n));
        assert buf.size() == 80;
        assertIndices(buf, 0, 0, 80);
        List<Integer> list=buf.removeMany(false,60);
        assert list.size() == 60;
        assert list.get(0) == 1 && list.get(list.size() -1) == 60;
        assertIndices(buf, 0, 60, 80);
        buf.purge(60);
        assertIndices(buf, 60, 60, 80);
        assert buf.size() == 20;
        if(buf instanceof DynamicBuffer) {
            ((DynamicBuffer<?>)buf).compact();
            assertIndices(buf, 60, 60, 80);
            assert buf.size() == 20;
            assertCapacity(buf.capacity(), ((DynamicBuffer<?>)buf).getNumRows(), 10);
        }
    }

    public void testCompact2(Buffer<Integer> type) {
        Buffer<Integer> buf=type instanceof DynamicBuffer? new DynamicBuffer<>(3,10,0) : new FixedBuffer<>(80, 0);

        int num_missing=type.numMissing();
        assert num_missing == 0;

        for(int i=1; i <= 80; i++)
             addAndGet(buf, i);
        assert buf.size() == 80;
        for(long i=1; i <= 60; i++)
            buf.remove();
        assert buf.size() == 20;
        if(buf instanceof DynamicBuffer) {
            ((DynamicBuffer<?>)buf).compact();
            assert buf.size() == 20;
            assertCapacity(buf.capacity(), ((DynamicBuffer<?>)buf).getNumRows(), 10);
        }
    }

    public void testSeqnoOverflow(Buffer<Message> buf) {
        _testSeqnoOverflow(buf, Long.MAX_VALUE - 10, 20);
        _testSeqnoOverflow(buf, -10, 20);
    }



    // ======================================== FixedBuffer tests only from here on ================================

    /**
     * Tests only {@link FixedBuffer}: creates a buf and fills it to capacity. Then starts a number of adder threads,
     * each trying to add a seqno, blocking until there is more space. Each adder will block until the remover removes
     * elements, so the adder threads get unblocked and can then add their elements to the buffer
     */
    public void testConcurrentAddAndRemove(Buffer<Integer> buf) throws Exception {
        if(!(buf instanceof FixedBuffer))
            return;
        buf=new FixedBuffer<>(16, 0);
        final int NUM=5, capacity=buf.capacity();
        for(int i=1; i <= capacity; i++)
            buf.add(i, i); // fill the buffer, add() would block from now on

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+17, buf);
            adders[i].start();
        }
        latch.countDown();
        System.out.print("waiting for adders to be done: ");

        Util.sleep(500);
        List<Integer> list=buf.removeMany(true, 5); // unblocks the 5 adders
        System.out.println("\nremoved = " + list);

        for(Adder adder: adders) {
            try {
                adder.join();
                assert adder.success();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("OK: buf = " + buf);
        assert buf.size() == 16;
        assertIndices(buf, 5, 5, 21);

        list=buf.removeMany(true, 0);
        System.out.println("removed = " + list);
        assert list.size() == 16;
        for(int i=6; i <=21; i++)
            assert list.contains(i);
        assertIndices(buf, 21, 21, 21);
    }

    public void testCapacity(Buffer<Integer> type) {
        if(!(type instanceof FixedBuffer))
            return;
        FixedBuffer<Integer> buf=new FixedBuffer<>(100, 1);
        System.out.println("buf = " + buf);
        assert buf.capacity() == 100;
        assert buf.isEmpty();
    }

    public void testAddBeyondCapacity(Buffer<Integer> buf) throws ExecutionException, InterruptedException {
        if(buf instanceof DynamicBuffer)
            return;
        buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= 10; i++)
            assert buf.add(i, i);
        final Buffer<Integer> b=buf;
        CompletableFuture<Boolean> cf=CompletableFuture.supplyAsync(() -> b.add(11, 11));
        Util.sleep(500);
        assert !cf.isDone();
        b.close();
        Util.sleep(100);
        assert cf.get() == false;
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
    }

    public void testBlockingAddAndRemove(Buffer<Integer> buf) throws ExecutionException, InterruptedException {
        if(buf instanceof DynamicBuffer)
            return;
        buf=new FixedBuffer<>(10, 0);
        for(int i=1; i <= 10; i++)
            assert buf.add(i, i);
        final Buffer<Integer> b=buf;
        CompletableFuture<Boolean> cf=CompletableFuture.supplyAsync(() -> b.add(11, 11));
        Util.sleep(500);
        assert !cf.isDone();
        Integer el=b.remove();
        assert el == 1;
        assert cf.get();
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
        List<Integer> actual=b.stream().collect(Collectors.toList()), expected=IntStream.rangeClosed(2,11).boxed().collect(Collectors.toList());
        assert actual.equals(expected);
    }

    public void testBlockingAddAndClose(Buffer<Integer> type) {
        if(type instanceof DynamicBuffer)
            return;
        FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, null, true);
        System.out.println("buf = " + buf);
        new Thread(() -> {
            Util.sleep(1000);
            buf.close();
        }).start();
        int seqno=buf.capacity() +1;
        boolean success=buf.add(seqno, seqno, null, true);
        System.out.println("buf=" + buf);
        assert !success;
        assert buf.size() == 10;
        assert buf.numMissing() == 0;

        List<Integer> list=buf.removeMany(true, 5);
        assert list.size() == 5;
        List<Integer> expected=IntStream.rangeClosed(1, 5).boxed().collect(Collectors.toList());
        assert expected.equals(list);

        boolean added=buf.add(11, 11);
        assert added && buf.size() == 6;

        for(int i=12; i <= 20; i++) // 16-20 will be dropped as open==false
            buf.add(i,i);
        assert buf.size() == 10;
        expected=IntStream.rangeClosed(6,15).boxed().collect(Collectors.toList());
        List<Integer> actual=buf.removeMany(true, 20);
        assert actual.equals(expected);
        assert buf.isEmpty();
        buf.open(true);
        for(int i=16; i <= 25; i++)
            buf.add(i,i);
        assert buf.size() == 10;
        expected=IntStream.rangeClosed(16,25).boxed().collect(Collectors.toList());
        actual=buf.removeMany(true, 20);
        assert actual.equals(expected);
    }

    public void testBlockingAddAndPurge(Buffer<Integer> type) throws InterruptedException {
        if(type instanceof DynamicBuffer)
            return;
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, null, true);
        System.out.println("buf = " + buf);
        Thread thread=new Thread(() -> {
            Util.sleep(1000);
            for(int i=0; i < 3; i++)
                buf.remove();
            buf.purge(3);
        });
        thread.start();
        boolean success=buf.add(11, 11, null, true);
        System.out.println("buf=" + buf);
        assert success;
        thread.join(10000);
        assert buf.size() == 8;
        assert buf.numMissing() == 0;
    }

    public void testBlockingAddAndPurge2(Buffer<Integer> type) throws TimeoutException {
        if(type instanceof DynamicBuffer)
            return;
        final FixedBuffer<Integer> buf=new FixedBuffer<>(10, 0);
        IntStream.rangeClosed(1, buf.capacity()).boxed()
          .forEach(n -> buf.add(n, n, null, true));
        System.out.println("buf = " + buf);
        BlockingAdder adder=new BlockingAdder(buf, 11, 14);
        adder.start();
        assert buf.size() == buf.capacity();
        boolean success=Util.waitUntilTrue(1000, 100, () -> adder.added() > 0);
        assert !success;
        Integer el=buf.remove();
        assert el == 1;
        Util.waitUntil(1000, 100, () -> adder.added() == 1);
        buf.purge(5, true);
        Util.waitUntil(1000, 100, () -> adder.added() == 4);
        assert buf.size() == 9;
    }

    public void testRemoveManyWithMissingElements(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,10).filter(n -> n!= 8).forEach(n -> buf.add(n,n));
        List<Integer> list=buf.removeMany(true, 3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(true, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;
    }

    public void testIterator(Buffer<Integer> buf) {
        IntStream.rangeClosed(1,10).forEach(n -> buf.add(n,n));
        int count=0;
        for(Integer num: buf) {
            if(num != null) {
                count++;
                System.out.print(num + " ");
            }
        }
        System.out.println();
        assert count == 10 : "count=" + count;
        boolean rc=buf.add(8, 8);
        assert rc == false;
        count=0;
        for(Integer num: buf) {
            if(num != null) {
                System.out.print(num + " ");
                count++;
            }
        }
        assert count == 10 : "count=" + count;
    }

    public void testIncreaseCapacity(Buffer<Integer> buf) {
        if(buf instanceof DynamicBuffer)
            return;
        int cap=buf.capacity();
        assert cap == 32;
        IntStream.rangeClosed(1, 32).forEach(n -> buf.add(n,n));
        ((FixedBuffer<Integer>)buf).changeCapacity(cap + 10);
        assert buf.capacity() == cap +10;
        assert buf.size() == 32;
    }

    public void testDecreaseCapacity(Buffer<Integer> buf) {
        if(buf instanceof DynamicBuffer)
            return;
        int cap=buf.capacity();
        assert cap == 32;
        IntStream.rangeClosed(1, 32).forEach(n -> buf.add(n,n));
        try {
            ((FixedBuffer<Integer>)buf).changeCapacity(cap / 2);
            assert false : "decreasing the capacity should have failed";
        }
        catch(IllegalStateException ex) {
            System.out.printf("received exception as expected: %s\n", ex);
        }

        buf.removeMany(true, 16);
        // now decreasing the buffer should succeed:
        ((FixedBuffer<Integer>)buf).changeCapacity(cap / 2);
        for(int i=17; i <= 32; i++)
            assert buf.get(i) == i;
    }

    protected static void _testSeqnoOverflow(Buffer<Message> type, long seqno, final int delta) {
        long orig_seqno=seqno;
        Buffer<Message> win=type instanceof DynamicBuffer? new DynamicBuffer<>(3, 10, seqno)
          : new FixedBuffer<>(seqno);

        for(int i=1; i <= delta; i++) {
            Message msg=new BytesMessage(null, "hello");
            win.add(++seqno, msg);
        }
        System.out.println("win = " + win);
        assert win.size() == delta;
        assertIndices(win, orig_seqno, orig_seqno, seqno);

        List<Message> msgs=win.removeMany(true, 200, null);
        System.out.printf("removed %d msgs\n", msgs.size());
        assert win.isEmpty();
        assertIndices(win, seqno, seqno, seqno);
    }


    protected static Message msg(int num) {return new ObjectMessage(null, num);}
    protected static Message msg(int num, boolean set_dont_loopback) {
        Message msg=msg(num);
        if(set_dont_loopback)
            msg.setFlag(DONT_LOOPBACK);
        return msg;
    }

    protected static void assertCapacity(int actual_capacity, int num_rows, int elements_per_row) {
        int actual_elements_per_row=Util.getNextHigherPowerOfTwo(elements_per_row);
        int expected_capacity=num_rows * actual_elements_per_row;
        assert actual_capacity == expected_capacity
          : "expected capacity of " + expected_capacity + " but got " + actual_capacity;
    }


    protected static void addAndGet(Buffer<Integer> buf, int ... seqnos) {
        for(int seqno: seqnos) {
            boolean added=buf.add(seqno, seqno);
            assert added;
            Integer val=buf.get(seqno);
            assert val != null && val == seqno;
        }
    }

    protected static <T> boolean add(Buffer<T> buf, List<LongTuple<T>> list) {
        boolean retval=false;
        for(LongTuple<T> tuple: list) {
            boolean rc=buf.add(tuple.getVal1(), tuple.getVal2());
            retval=retval || rc;
        }
        return retval;
    }

    protected static boolean add(Buffer<Integer> b, int start, int end) {
        boolean retval=true;
        for(int i=start; i <= end; i++)
            retval=retval || b.add(i, i);
        return retval;
    }

    protected static List<LongTuple<Integer>> createList(long ... seqnos) {
        if(seqnos == null)
            return null;
        List<LongTuple<Integer>> msgs=new ArrayList<>(seqnos.length);
        for(long seqno: seqnos)
            msgs.add(new LongTuple<>(seqno, (int)seqno));
        return msgs;
    }

    protected static MessageBatch createMessageBatch(long ... seqnos) {
        if(seqnos == null)
            return null;
        MessageBatch mb=new MessageBatch(seqnos.length);
        for(long seqno: seqnos) {
            Message m=new ObjectMessage(null, seqno).putHeader(NAKACK3_ID, NakAckHeader.createMessageHeader(seqno));
            mb.add(m);
        }
        return mb;
    }

    protected static MessageBatch createMessageBatch(int start, int end, boolean oob) {
        MessageBatch mb=new MessageBatch(end-start+1);
        for(long seqno=start; seqno <= end; seqno++) {
            Message m=new ObjectMessage(null, seqno).putHeader(NAKACK3_ID, NakAckHeader.createMessageHeader(seqno));
            if(oob)
                m.setFlag(OOB);
            mb.add(m);
        }
        return mb;
    }

    protected static <T> void assertIndices(Buffer<T> buf, long low, long hd, long hr) {
        assert buf.low() == low : "expected low=" + low + " but was " + buf.low();
        assert buf.highestDelivered() == hd : "expected hd=" + hd + " but was " + buf.highestDelivered();
        assert buf.high()  == hr : "expected hr=" + hr + " but was " + buf.high();
    }

    protected static class Adder extends Thread {
        protected final CountDownLatch  latch;
        protected final int             seqno;
        protected final Buffer<Integer> buf;
        protected boolean               success;

        public Adder(CountDownLatch latch, int seqno, Buffer<Integer> buf) {
            this.latch=latch;
            this.seqno=seqno;
            this.buf=buf;
        }

        public boolean success() {
            return success;
        }

        public void run() {
            try {
                latch.await();
                success=buf.add(seqno, seqno, null, true);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected static class BlockingAdder extends Thread {
        protected final Buffer<Integer> buf;
        protected final int             from, to;
        protected int                   added;

        protected BlockingAdder(FixedBuffer<Integer> buf, int from, int i) {
            this.buf=buf;
            this.from=from;
            to=i;
        }

        public int added() {return added;}

        @Override
        public void run() {
            for(int i=from; i <= to; i++) {
                boolean rc=buf.add(i, i, null, true);
                if(rc) {
                    added++;
                    System.out.printf("-- added %d\n", i);
                }
            }
            System.out.printf("-- done, added %d elements\n", added);
        }
    }

}
