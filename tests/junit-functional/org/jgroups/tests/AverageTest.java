package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Average;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * @author Bela Ban
 * @since  3.6.8
 */
@Test(groups=Global.FUNCTIONAL)
public class AverageTest {
    // 20% between expected and actual averages in allowed
    protected static final double DEVIATION=0.2;

    public void testAverage() {
        long[] numbers=new long[1000];
        long total=0;

        for(int i=0; i < numbers.length; i++) {
            numbers[i]=Util.random(10000L);
            total+=numbers[i];
        }

        double expected_avg=total/1000.0;

        Average a=new Average(1000);
        for(long num: numbers)
            a.add(num);

        double avg=a.average();

        expected_avg=Math.floor(expected_avg);
        avg=Math.floor(avg);
        assert Util.withinRange(avg, expected_avg, DEVIATION);
    }

    public void testAverage2() {
        Average avg=new Average(16);
        double expected_avg=IntStream.rangeClosed(5, 20).sum() / 16.0;
        IntStream.rangeClosed(1,20).forEach(avg::add);
        double actual_avg=avg.average();
        assert Util.withinRange(actual_avg, expected_avg, DEVIATION) : String.format("actual: %.2f expected: %.2f\n", actual_avg, expected_avg);
    }


    public void testOverflow() {
        long start=Long.MAX_VALUE/ 500;
        Average avg=new Average();
        for(int i=0; i < 1000; i++)
            avg.add(start++);
        long cnt=avg.count();
        System.out.printf("cnt=%d, avg=%.2f\n", cnt, avg.average());
    }

    public void testMinMax() {
        AverageMinMax avg=new AverageMinMax();
        IntStream.rangeClosed(1,10).forEach(avg::add);
        double average=IntStream.rangeClosed(1,10).average().orElse(0.0);
        assert Util.withinRange(avg.average(), average, 0.2);
        assert avg.min() == 1;
        assert avg.max() == 10;
    }

    public void testIsEmpty() {
        Average avg=new Average(10);
        assert avg.isEmpty();
        avg.add(5);
        assert !avg.isEmpty();
        while(avg.count() != avg.capacity())
            avg.add(5);
        assert !avg.isEmpty();
        assert avg.count() == avg.capacity();
        assert avg.average() == 5.0;
    }

    public void testMerge() {
        AverageMinMax avg1=new AverageMinMax(11000), avg2=new AverageMinMax(10000);
        IntStream.rangeClosed(1, 1000).forEach(i -> avg1.add(1));
        IntStream.rangeClosed(1, 10000).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert Util.withinRange(1.9, avg1.average(), 0.1);
        assert avg1.min() == 1;
        assert avg1.max() == 2;
    }

    public void testMerge2() {
        AverageMinMax avg1=new AverageMinMax(10000), avg2=new AverageMinMax(10000);
        IntStream.rangeClosed(1, 10000).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.average() == 2.0;
        assert avg1.min() == 2;
        assert avg1.max() == 2;
    }

    public void testMerge3() {
        AverageMinMax avg1=new AverageMinMax(100), avg2=new AverageMinMax(200);
        IntStream.rangeClosed(1, 100).forEach(i -> avg1.add(1));
        IntStream.rangeClosed(1, 200).forEach(i -> avg2.add(2));
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert Util.withinRange(avg1.average(), 2.0, 0.2);
        assert avg1.min() == 1;
        assert avg1.max() == 2;
    }

    public void testAverageWithNoElements() {
        Average avg=new AverageMinMax();
        double av=avg.average();
        assert av == 0.0;
    }

    public void testSerialization() throws IOException, ClassNotFoundException {
        Average avg=new Average(128);
        for(int i=0; i < 100; i++)
            avg.add(Util.random(128));
        ByteArray buf=Util.objectToBuffer(avg);
        Average avg2=Util.objectFromBuffer(buf, null);
        assert avg2 != null;
        assert avg.count() == avg2.count();
        assert avg.average() == avg2.average();
    }

}
