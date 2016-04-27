package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Average;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;
import org.testng.annotations.Test;


/**
 * @author Bela Ban
 * @since  3.6.8
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=false)
public class AverageTest {

    public void testAverage() {
        long[] numbers=new long[1000];
        long total=0;

        for(int i=0; i < numbers.length; i++) {
            numbers[i]=Util.random(10000);
            total+=numbers[i];
        }

        double expected_avg=total/1000.0;

        Average a=new Average();
        for(long num: numbers)
            a.add(num);

        double avg=a.getAverage();

        expected_avg=Math.floor(expected_avg);
        avg=Math.floor(avg);

        assert avg == expected_avg;
    }


    public void testOverflow() {
        long start=Long.MAX_VALUE/ 500;
        Average avg=new Average();
        for(int i=0; i < 1000; i++)
            avg.add(start++);

        long cnt=avg.getCount();
        System.out.printf("cnt=%d, avg=%.2f\n", cnt, avg.getAverage());
        assert cnt == 500; // was reset at i=500
    }

    public void testMinMax() {
        AverageMinMax avg=new AverageMinMax();
        for(int i=1; i <= 10; i++)
            avg.add(i);
        assert avg.getAverage() == 5.5;
        assert avg.min() == 1;
        assert avg.max() == 10;
    }

    public void testMerge() {
        AverageMinMax avg1=new AverageMinMax(), avg2=new AverageMinMax();
        for(int i=1; i <= 1000; i++)
            avg1.add(1);
        for(int i=1; i <= 10000; i++)
            avg2.add(2);
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.count() == 5500;
        double diff=Math.abs(avg1.getAverage() - 1.90);
        assert diff < 0.01;
        assert avg1.min() == 1;
        assert avg1.max() == 2;
    }

    public void testMerger2() {
        AverageMinMax avg1=new AverageMinMax(), avg2=new AverageMinMax();
        for(int i=1; i <= 10000; i++)
            avg2.add(2);
        System.out.printf("avg1: %s, avg2: %s\n", avg1, avg2);
        avg1.merge(avg2);
        System.out.printf("merged avg1: %s\n", avg1);
        assert avg1.count() == 5000;
        assert avg1.average() == 2.0;
        assert avg1.min() == 2;
        assert avg1.max() == 2;
    }

}
