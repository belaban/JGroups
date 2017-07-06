package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.util.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.util.ResponseCollectorTask}
 * @author Bela Ban
 * @since  4.0.5
 */
@Test
public class ResponseCollectorTaskTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D"), e=Util.createRandomAddress("E");
    protected final TimeScheduler timer=new TimeScheduler3(new DefaultThreadFactory("test", false),
                                                           0, 10, 5000, 100, "abort");

    @AfterClass
    protected void destroy() {
        timer.stop();
    }

    public void testTask() {
        ResponseCollectorTask<Boolean> task=new ResponseCollectorTask<>(a,b,c,d,e);
        task.setPeriodicTask(t -> {
            System.out.printf(".");Util.sleep(200);});
        assert !task.hasAllResponses();
        task.start(timer, 500, 200);
        Util.sleep(1000);
        assert !task.isDone();
        Stream.of(a,b,c,d,e).peek(mbr -> System.out.printf("adding %s\n", mbr)).forEach(mbr -> task.add(mbr, true));
        assert task.isDone();
        assert task.hasAllResponses();
    }

    public void testRetainAll() {
        ResponseCollectorTask<Boolean> task=new ResponseCollectorTask<>(a,b,c,d,e);
        task.setPeriodicTask(t -> {
            System.out.printf(".");Util.sleep(200);});
        assert !task.hasAllResponses();
        task.start(timer, 500, 200);
        Stream.of(a,b,d).peek(mbr -> System.out.printf("adding %s\n", mbr)).forEach(mbr -> task.add(mbr, true));
        assert !task.isDone();
        assert !task.hasAllResponses();

        task.retainAll(Arrays.asList(a,b,d,e));
        assert !task.isDone();
        assert !task.hasAllResponses();

        task.retainAll(Arrays.asList(a,b,d));
        assert task.isDone();
        assert task.hasAllResponses();
    }

    public void testStop() {
        ResponseCollectorTask<Boolean> task=new ResponseCollectorTask<>(a,b,c,d,e);
        task.setPeriodicTask(t -> Util.sleep(20000));
        task.start(timer, 500, 200);
        Util.sleep(2000);
        task.stop();
        assert task.isDone();
        assert !task.hasAllResponses();
    }

    public void testEmptyTargetSet() {
        ResponseCollectorTask<Boolean> task=new ResponseCollectorTask<>();
        task.setPeriodicTask(t -> Util.sleep(100));
        task.start(timer, 500, 200);
        // Util.sleep(2000);
        assert task.isDone();
    }
}
