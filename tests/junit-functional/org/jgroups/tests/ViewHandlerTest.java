package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.ViewHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
        GMS gms=new GMS();
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


    protected void configureGMS(GMS gms) {
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

    protected void set(GMS gms, String field, Object value) {
        Field f=Util.getField(gms.getClass(), field);
        Util.setField(f, gms, value);
    }
}
