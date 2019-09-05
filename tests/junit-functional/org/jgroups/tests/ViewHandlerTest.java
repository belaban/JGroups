package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.GmsImpl;
import org.jgroups.protocols.pbcast.ViewHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link ViewHandler}
 * @author Bela Ban
 * @since  4.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ViewHandlerTest {
    protected ViewHandler<Integer>            view_handler;
    protected Consumer<Collection<Integer>>   req_handler;
    protected BiPredicate<Integer,Integer>    req_matcher;
    protected GMS                             gms;

    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");

    protected void process(Collection<Integer> requests) {
        if(req_handler != null)
            req_handler.accept(requests);
    }

    // either both even or both odd
    protected static boolean matches(int a, int b) {
        return (a % 2 == 0 && b % 2 == 0) || (a %2 != 0 && b % 2 != 0);
    }

    protected boolean match(Integer i1, Integer i2) {
        return req_matcher != null && req_matcher.test(i1, i2);
    }

    @BeforeMethod protected void init() {
        gms=new GMS();
        configureGMS(gms);
        view_handler=new ViewHandler<>(gms, this::process, this::match);
    }

    public void testAdd() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=ViewHandlerTest::matches;
        Stream.of(1,3,5,2,4,6).forEach(n -> view_handler.add(n));
        System.out.printf("list: %s\n", list);
        assert list.size() == 6;
        assert list.equals(Arrays.asList(1,3,5,2,4,6));
    }

    public void testAdd2() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=(a,b) -> (a % 2 == 0 && b % 2 == 0) || (a %2 != 0 && b % 2 != 0); // either both even or both odd
        Stream.of(1,3,5,2,4,6,7).forEach(n -> view_handler.add(n));
        System.out.println("list: " + list);
        assert list.equals(Arrays.asList(1,3,5,2,4,6,7));
    }


    public void testAddList() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=ViewHandlerTest::matches; // either both even or both odd
        Integer[] numbers={1,3,5,2,4,6,7,9,8,10};
        view_handler.add(numbers);
        System.out.printf("drained: %s\n", list);
        assert list.equals(Arrays.asList(numbers));
    }

    public void testConcurrentAdd() throws Exception {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=(a,b) -> true;

        final CountDownLatch latch=new CountDownLatch(1);
        Thread[] adders=new Thread[20];
        for(int i=0; i < adders.length; i++) {
            final int num=i;
            adders[i]=new Thread(() -> {
                try {
                    latch.await();
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
                // Util.sleepRandom(10,200);
                view_handler.add(num+1);
            });
            adders[i].start();
        }
        Util.sleep(200);
        latch.countDown();
        for(Thread t: adders)
            t.join(3000);
        System.out.println("list: " + list);
        assert list.size() == 20;
    }


    public void testAddInTwoBatches() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=(a,b) -> true;
        Integer[] first={1,2,3,4,5,6,7,8,9,10}, second={11,12,13,14,15,16,17,18,19,20};
        view_handler.add(first);
        System.out.printf("list: %s\n", list);
        assert list.size() == 10;
        assert list.equals(Arrays.asList(first));

        view_handler.add(second);
        assert list.size() == 20;
        System.out.printf("list: %s\n", list);
        for(int i=0; i < 20; i++)
            assert list.get(i) == i+1;
    }

    public void testAddNoMatches() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=ViewHandlerTest::matches;
        Integer[] numbers={1,2,3,4,5,6,7,8,9,10};
        view_handler.add(numbers);
        System.out.printf("list: %s\n", list);
        assert list.size() == 10;
        assert list.equals(Arrays.asList(numbers));
    }


    public void testSuspendResume() {
        List<Integer> list=new ArrayList<>();
        req_handler=list::addAll;
        req_matcher=(a,b) -> true;
        view_handler.suspend();
        IntStream.rangeClosed(1,10).forEach(view_handler::add);
        assert list.isEmpty();

        view_handler.resume();
        IntStream.rangeClosed(1,10).forEach(view_handler::add);
        assert !list.isEmpty();
        assert list.equals(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
    }

    public void testDuplicateRequestsJoin() {
        Collection<GmsImpl.Request> reqs=new LinkedHashSet<>();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.JOIN, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.JOIN, b));
        assert reqs.size() == 2;
        reqs.add(new GmsImpl.Request(GmsImpl.Request.JOIN, a));
        assert reqs.size() == 2 : "requests: "+ reqs;

        reqs.clear();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.JOIN, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.JOIN_WITH_STATE_TRANSFER, a));
        assert reqs.size() == 2;
    }


    public void testDuplicateRequestsLeave() {
        Collection<GmsImpl.Request> reqs=new LinkedHashSet<>();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, b));
        assert reqs.size() == 2;
        reqs.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, a));
        assert reqs.size() == 2 : "requests: "+ reqs;

        reqs.clear();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.SUSPECT, a));
        assert reqs.size() == 2;
    }

    public void testDuplicateRequestsSuspect() {
        Collection<GmsImpl.Request> reqs=new LinkedHashSet<>();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.SUSPECT, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.SUSPECT, b));
        assert reqs.size() == 2;
        reqs.add(new GmsImpl.Request(GmsImpl.Request.SUSPECT, a));
        assert reqs.size() == 2 : "requests: "+ reqs;

        reqs.clear();
        reqs.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, a));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.SUSPECT, a));
        assert reqs.size() == 2;
    }

    public void testDuplicateRequestsMerge() {
        Collection<GmsImpl.Request> reqs=new LinkedHashSet<>();

        Map<Address,View> map1=new HashMap<>();
        map1.put(a, View.create(a, 1, a,b));

        Map<Address,View> map2=new HashMap<>();
        map2.put(b, View.create(b, 2, b,a));

        reqs.add(new GmsImpl.Request(GmsImpl.Request.MERGE, null, map1));
        reqs.add(new GmsImpl.Request(GmsImpl.Request.MERGE, null, map2));
        assert reqs.size() == 2;
        reqs.add(new GmsImpl.Request(GmsImpl.Request.MERGE, null, map1));
        assert reqs.size() == 2 : "requests: "+ reqs;
    }

    public void testCount() {
        view_handler.add(1).add(2).add(3).add(1);
        Util.sleep(1000);
        assert view_handler.size() == 0;
    }

    public void testCount2() {
        view_handler.add(1,2,3,1,1);
        Util.sleep(1000);
        assert view_handler.size() == 0;
    }

    public void testWaitUntilCompleteOnEmptyQueue() {
        view_handler.waitUntilComplete(10000);
        System.out.println("view_handler = " + view_handler);
    }

    public void testWaitUntilCompleteOnEmptyQueue2() {
        view_handler.add(10);
        view_handler.waitUntilComplete(10000);
        System.out.println("view_handler = " + view_handler);
       }

    // @Test(invocationCount=10)
    public void testWaitUntilComplete() throws Exception {
        req_handler=list -> list.forEach(n -> Util.sleep(30));
        Adder[] adders=new Adder[10];
        final CountDownLatch latch=new CountDownLatch(1);
        int j=0;
        for(int i=0; i < adders.length; i++, j+=10) {
            adders[i]=new Adder(j, j+10, view_handler, latch);
            adders[i].start();
        }
        latch.countDown();
        for(Adder adder: adders) {
            long start=System.currentTimeMillis();
            adder.join(2000);
            long time=System.currentTimeMillis()-start;
            System.out.printf("Joined %d in %d ms\n", adder.getId(), time);
        }
        System.out.println("view_handler = " + view_handler);
        view_handler.waitUntilComplete(10000);
        // view_handler.waitUntilComplete();
        System.out.println("view_handler = " + view_handler);
        assert view_handler.size() == 0;
    }

    public void testCoordLeave() {
        final AtomicBoolean result=new AtomicBoolean(true);
        Consumer<Collection<GmsImpl.Request>> req_processor=l -> {
            System.out.printf("setting result to %b: list: %s\n", l.size() < 2, l);
            if(l.size() >=2)
                result.set(false);
        };
        ViewHandler<GmsImpl.Request> handler=new ViewHandler<>(gms, req_processor, GmsImpl.Request::canBeProcessedTogether);
        handler.add(new GmsImpl.Request(GmsImpl.Request.COORD_LEAVE),
                    new GmsImpl.Request(GmsImpl.Request.COORD_LEAVE));

        assert result.get();
    }

    public void testCoordLeave2() {
        final AtomicBoolean result=new AtomicBoolean(true);
        Consumer<Collection<GmsImpl.Request>> req_processor=l -> {
            int num_coord_leave_req=(int)l.stream().filter(req -> req.getType() == GmsImpl.Request.COORD_LEAVE).count();
            System.out.printf("setting result to %b: list: %s\n", num_coord_leave_req < 2, l);
            if(num_coord_leave_req >=2)
                result.set(false);
        };
        ViewHandler<GmsImpl.Request> handler=new ViewHandler<>(gms, req_processor, GmsImpl.Request::canBeProcessedTogether);
        handler.add(new GmsImpl.Request(GmsImpl.Request.LEAVE, a),
                    new GmsImpl.Request(GmsImpl.Request.JOIN, b),
                    new GmsImpl.Request(GmsImpl.Request.COORD_LEAVE),
                    new GmsImpl.Request(GmsImpl.Request.COORD_LEAVE));

        assert result.get();
    }


    protected static void configureGMS(GMS gms) {
        Address local_addr=Util.createRandomAddress("A");
        ThreadFactory fac=new DefaultThreadFactory("test", true);
        set(gms, "local_addr", local_addr);
        set(gms, "timer", new TimeScheduler3(fac, 1, 3, 5000, 100, "run"));
        gms.setDownProtocol(new Protocol() {
            public ThreadFactory getThreadFactory() {
                return fac;
            }
        });
    }

    protected static void set(GMS gms, String field, Object value) {
        Field f=Util.getField(gms.getClass(), field);
        Util.setField(f, gms, value);
    }

    protected static class Adder extends Thread {
        protected final int            from, to;
        protected final ViewHandler    vh;
        protected final CountDownLatch latch;

        public Adder(int from, int to, ViewHandler vh, CountDownLatch latch) {
            this.from=from;
            this.to=to;
            this.vh=vh;
            this.latch=latch;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
            }

            int len=to-from+1;
            Object[] numbers=new Integer[len];
            for(int i=0; i < numbers.length; i++)
                numbers[i]=from+i;

            // IntStream.rangeClosed(from, to).forEach(vh::add);
            vh.add(numbers);
        }
    }
}
