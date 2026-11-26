package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test cases for AgeOutCache
 * 
 * @author Bela Ban
 */
@Test(groups = Global.FUNCTIONAL,singleThreaded=true)
public class AgeOutCacheTest {

    @DataProvider(name="timerCreator")
    protected Object[][] timerCreator() {
        return Util.createTimer();
    }


    @Test(dataProvider="timerCreator")
    public void testExpiration(TimeScheduler timer) {
        AgeOutCache<Integer> cache=new AgeOutCache<>(timer, 2000L,
                                                     (AgeOutCache.Handler<Integer>)key -> System.out.println(key + " expired"));
        for(int i = 1; i <= 5; i++)
            cache.add(i);
        int size=cache.size();
        System.out.println("cache:\n" + cache);
        assert size == 5 : "size is " + size;

        for(int i=0; i < 30; i++) {
            if(cache.size() == 0)
                break;
            Util.sleep(1000);
        }
        System.out.println("cache:\n" + cache);
        size=cache.size();
        assert size == 0 : "size is " + size;
        timer.stop();
    }


    @Test(dataProvider="timerCreator")
    public void testRemoveAndExpiration(TimeScheduler timer) {
        AgeOutCache<Integer> cache = new AgeOutCache<>(timer, 2000L);
        for (int i = 1; i <= 5; i++)
            cache.add(i);
        System.out.println("cache:\n" + cache);
        cache.remove(3);
        cache.remove(5);
        cache.remove(6); // not existent

        boolean correct_size=false;
        for(int i=0; i < 20; i++) {
            if(cache.size() == 3) {
                correct_size=true;
                break;
            }
            Util.sleep(500);
        }

        if(!correct_size) {
            System.out.println("cache:\n" + cache);
            assert cache.size() == 3 : "cache size should be 3 but is " + cache.size();
        }

        for(int i=0; i < 10; i++) {
            if(cache.size() == 0)
                break;
            Util.sleep(1000);
        }

        assert cache.size() == 0;
        timer.stop();
    }


    @Test(dataProvider="timerCreator")
    public void testContains(TimeScheduler timer) {
        AgeOutCache<Integer> cache = new AgeOutCache<>(timer, 5000L);
        for (int i = 1; i <= 5; i++)
            cache.add(i);
        System.out.println("cache:\n" + cache);
        assert cache.contains(3);
        cache.remove(3);
        System.out.println("cache:\n" + cache);
        assert !cache.contains(3);
        cache.clear();
        assert cache.size() == 0;
        assert !cache.contains(4);
        timer.stop();
    }


    @Test(dataProvider="timerCreator")
    public void testGradualExpiration(TimeScheduler timer) {
        AgeOutCache<Integer> cache = new AgeOutCache<>(timer, 5000L,
                                                       (AgeOutCache.Handler<Integer>)key -> System.out.println(key + " expired"));
        for (int i = 1; i <= 10; i++) {
            cache.add(i);
            System.out.print(".");
            Util.sleep(1000);
        }
        System.out.println("\ncache:\n" + cache);
        int size = cache.size();
        assert size < 10 && size > 0 : " size was " + size + ", but should have been < 10 or > 0";
        timer.stop();
    }
}