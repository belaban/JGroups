package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Average;
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

}
