package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.RingBufferSeqno;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Functional tests of RingBuffer
 * @author Bela Ban
 * @since 3.1
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},description="Functional tests of RingBuffer")
public class RingBufferSeqnoTest {

    public void testConstructor() {
        RingBufferSeqno buf=new RingBufferSeqno(100, 1);
        System.out.println("buf = " + buf);
        assert buf.capacity() == Util.getNextHigherPowerOfTwo(100);
        assert buf.size() == 0;
    }

    public void testIndex() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 5);
        assert buf.getHighestDelivered() == 5;
        assert buf.getHighestReceived() == 5;
        buf.add(6,6); buf.add(7,7);
        buf.remove(); buf.remove();
        long low=buf.getLow();
        buf.stable(4);
        buf.stable(5);
        buf.stable(6);
        buf.stable(7);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testIndexWithRemoveMany() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 5);
        assert buf.getHighestDelivered() == 5;
        assert buf.getHighestReceived() == 5;
        buf.add(6,6); buf.add(7,7);
        long low=buf.getLow();
        buf.removeMany(true,0);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        assertIndices(buf, 7, 7, 7);
    }

    public void testAddWithInvalidSeqno() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(100, 20);
        assert buf.add(10, 0) == false;
        assert buf.add(20, 0) == false;
        assert buf.size() == 0;
    }

    public void testAdd() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        buf.add(1, 322649);
        buf.add(2, 100000);
        System.out.println("buf = " + buf);
        assert buf.size() == 2;
    }

    public void testSaturation() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,8))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int size=buf.size(), space_used=buf.spaceUsed();
        double saturation=buf.saturation();
        System.out.println("size=" + size + ", space used=" + space_used + ", saturation=" + saturation);
        assert buf.size() == 8;
        assert buf.spaceUsed() == 8;
        assert buf.saturation() == 0.5;

        buf.remove(); buf.remove(); buf.remove();
        size=buf.size();
        space_used=buf.spaceUsed();
        saturation=buf.saturation();
        System.out.println("size=" + size + ", space used=" + space_used + ", saturation=" + saturation);
        assert buf.size() == 5;
        assert buf.spaceUsed() == 8;
        assert buf.saturation() == 0.5;

        long low=buf.getLow();
        buf.stable(3);
        for(long i=low; i <= 3; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        size=buf.size();
        space_used=buf.spaceUsed();
        saturation=buf.saturation();
        System.out.println("size=" + size + ", space used=" + space_used + ", saturation=" + saturation);
        assert buf.size() == 5;
        assert buf.spaceUsed() == 5;
        assert buf.saturation() == 0.3125;
    }

    public void testAddWithWrapAround() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        for(int i=0; i < 3; i++) {
            Integer val=buf.remove();
            System.out.println("removed " + val);
            assert val != null;
        }
        System.out.println("buf = " + buf);

        long low=buf.getLow();
        buf.stable(8);
        System.out.println("buf = " + buf);
        assert buf.getLow() == 8;
        for(long i=low; i <= 8; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        while(buf.remove() != null)
            ;
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        assert buf.missing() == 0;
        low=buf.getLow();
        buf.stable(18);
        assert buf.getLow() == 18;
        for(long i=low; i <= 18; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }

    public void testAddWithWrapAroundAndRemoveMany() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        List<Integer> removed=buf.removeMany(true,3);
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

        assert buf.size() == 0;
        assert buf.missing() == 0;
        assertIndices(buf, 18, 18, 18);
    }

    public void testAddBeyondCapacity() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i=1; i <=10; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);
    }

    public void testAddMissing() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,4,5,6))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 5 && buf.missing() == 1;

        Integer num=buf.remove();
        assert num == 1;
        num=buf.remove();
        assert num == 2;
        num=buf.remove();
        assert num == null;

        buf.add(3, 3);
        System.out.println("buf = " + buf);
        assert buf.size() == 4 && buf.missing() == 0;

        for(int i=3; i <= 6; i++) {
            num=buf.remove();
            System.out.println("buf = " + buf);
            assert num == i;
        }

        num=buf.remove();
        assert num == null;
    }


    public void testGetMissing() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(30, 0);
        for(int i: Arrays.asList(2,5,10,11,12,13,15,20,28,30))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int missing=buf.missing();
        System.out.println("missing=" + missing);
        SeqnoList missing_list=buf.getMissing();
        System.out.println("missing_list = " + missing_list);
        assert missing_list.size() == missing;
    }

    public void testGetMissing2() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        buf.add(1,1);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing == null && buf.missing() == 0;

        buf=new RingBufferSeqno<>(10, 0);
        buf.add(10,10);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.missing() == missing.size();

        buf=new RingBufferSeqno<>(10, 0);
        buf.add(5,5);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.missing() == missing.size();

        buf=new RingBufferSeqno<>(10, 0);
        buf.add(5,7);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.missing() == missing.size();
    }

    public void testBlockingAddAndDestroy() {
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, true);
        System.out.println("buf = " + buf);
        new Thread() {
            public void run() {
                Util.sleep(1000);
                buf.destroy();
            }
        }.start();
        int seqno=buf.capacity() +1;
        boolean success=buf.add(seqno, seqno, true);
        System.out.println("buf=" + buf);
        assert !success;
        assert buf.size() == 10;
        assert buf.missing() == 0;
    }

    public void testBlockingAddAndStable() throws InterruptedException {
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i=0; i <= 10; i++)
            buf.add(i, i, true);
        System.out.println("buf = " + buf);
        Thread thread=new Thread() {
            public void run() {
                Util.sleep(1000);
                for(int i=0; i < 3; i++)
                    buf.remove();
                buf.stable(3);
            }
        };
        thread.start();
        boolean success=buf.add(11, 11, true);
        System.out.println("buf=" + buf);
        assert success;
        thread.join(10000);
        assert buf.size() == 8;
        assert buf.missing() == 0;
    }

    public void testGet() {
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        assert buf.get(0) == null;
        assert buf.get(1) == 1;
        assert buf.get(10) == null;
        assert buf.get(5) == 5;
        assert buf.get(6) == null;
    }

    public void testGetList() {
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        List<Integer> elements=buf.get(3,5);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 3;
        assert elements.contains(3) && elements.contains(4) && elements.contains(5);

        elements=buf.get(4, 10);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 2;
        assert elements.contains(4) && elements.contains(5);

        elements=buf.get(10, 20);
        assert elements == null;
    }

    public void testRemove() {
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
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

    public void testRemovedPastHighestReceived() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        int highest=buf.capacity();
        for(int i=1; i <= 20; i++) {
            if(i > highest) {
                assert  !buf.add(i,i);
                Integer num=buf.remove();
                assert num == null;
            }
            else {
                assert  buf.add(i,i);
                Integer num=buf.remove();
                assert num != null && num == i;
            }
        }
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        assert buf.missing() == 0;
    }

    public void testRemoveMany() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
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

    public void testRemoveManyWithNulling() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
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

        list=buf.removeMany(false, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;
    }

    /**
     * Runs NUM adder threads, each adder adds 1 (unique) seqno. When all adders are done, we should have
     * NUM elements in the RingBuffer.
     */
    public void testConcurrentAdd() {
        final int NUM=100;
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(NUM, 0);

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+1, buf);
            adders[i].start();
        }

        Util.sleep(1000);
        System.out.println("releasing threads");
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
        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == NUM;
    }

    /**
     * Creates a RingBuffer and fills it to capacity. Then starts a number of adder threads, each trying to add a
     * seqno, blocking until there is more space. Each adder will block until the remover removes elements, so the
     * adder threads get unblocked and can then add their elements to the buffer.
     */
    public void testConcurrentAddAndRemove() throws InterruptedException {
        final int NUM=5;
        final RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i=1; i <= 10; i++)
            buf.add(i, i); // fill the buffer, add() will block now

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+11, buf);
            adders[i].start();
        }

        Util.sleep(1000);
        System.out.println("releasing threads");
        latch.countDown();
        System.out.print("waiting for threads to be done: ");

        Thread remover=new Thread("Remover") {
            public void run() {
                Util.sleep(2000);
                List<Integer> list=buf.removeMany(true, 5);
                System.out.println("\nremover: removed = " + list);
            }
        };
        remover.start();

        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        remover.join(10000);

        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == 10;
        assertIndices(buf, 5, 5, 15);

        List<Integer> list=buf.removeMany(true, 0);
        System.out.println("removed = " + list);
        assert list.size() == 10;
        for(int i=6; i <=15; i++)
            assert list.contains(i);
        assertIndices(buf, 15, 15, 15);
    }

    public void testStable() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i=1; i <=7; i++) {
            buf.add(i, i);
            buf.remove();
        }
        System.out.println("buf = " + buf);
        assert buf.size() == 0;
        long low=buf.getLow();
        buf.stable(3);
        assert buf.getLow() == 3;
        for(long i=low; i <= 3; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";


        buf.stable(6);
        assert buf.get(6) == null;
        buf.stable(7);
        assert buf.get(7) == null;
        assert buf.getLow() == 7;
        assert buf.size()  == 0;

        for(int i=7; i <= 14; i++) {
            buf.add(i, i);
            buf.remove();
        }

        System.out.println("buf = " + buf);
        assert buf.size() == 0;

        low=buf.getLow();
        buf.stable(12);
        System.out.println("buf = " + buf);
        assert buf.getLow() == 12;
        for(long i=low; i <= 12; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }
    

    public void testIterator() {
        RingBufferSeqno<Integer> buf=new RingBufferSeqno<>(10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        int count=0;
        for(Integer num: buf) {
            if(num != null) {
                count++;
                System.out.print(num + " ");
            }
        }
        System.out.println();
        assert count == 9 : "count=" + count;
        buf.add(8,8);
        count=0;
        for(Integer num: buf) {
            if(num != null) {
                System.out.print(num + " ");
                count++;
            }
        }
        assert count == 10 : "count=" + count;
    }

    protected static <T> void assertIndices(RingBufferSeqno<T> buf, long low, long hd, long hr) {
        assert buf.getLow() == low : "expected low=" + low + " but was " + buf.getLow();
        assert buf.getHighestDelivered() == hd : "expected hd=" + hd + " but was " + buf.getHighestDelivered();
        assert buf.getHighestReceived()  == hr : "expected hr=" + hr + " but was " + buf.getHighestReceived();
    }

    protected static class Adder extends Thread {
        protected final CountDownLatch latch;
        protected final int seqno;
        protected final RingBufferSeqno<Integer> buf;

        public Adder(CountDownLatch latch, int seqno, RingBufferSeqno<Integer> buf) {
            this.latch=latch;
            this.seqno=seqno;
            this.buf=buf;
        }

        public void run() {
            try {
                latch.await();
                Util.sleepRandom(10, 500);
                buf.add(seqno, seqno, true);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
