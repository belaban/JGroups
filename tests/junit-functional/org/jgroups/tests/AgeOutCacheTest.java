package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Test cases for AgeOutCache
 * 
 * @author Bela Ban
 * @version $Id: AgeOutCacheTest.java,v 1.9 2009/07/20 16:46:15 belaban Exp $
 */
@Test(groups = Global.FUNCTIONAL, sequential = true)
public class AgeOutCacheTest {
    ScheduledExecutorService timer;

    @BeforeClass
    void start() throws Exception {
        timer=Executors.newScheduledThreadPool(5);
    }

    @AfterClass
    void stop() throws Exception {
        timer.shutdownNow();
    }

    public void testExpiration() {
        AgeOutCache<Integer> cache=new AgeOutCache<Integer>(timer, 500L,
                                                            new AgeOutCache.Handler<Integer>() {
                                                                public void expired(Integer key) {
                                                                    System.out.println(key + " expired");
                                                                }
                                                            });
        for(int i = 1; i <= 5; i++)
            cache.add(i);
        System.out.println("cache:\n" + cache);
        int size=cache.size();
        assert size == 5 : "size is " + size;
        Util.sleep(1500);
        System.out.println("cache:\n" + cache);
        assert (size=cache.size()) == 0 : "size is " + size;
    }

    public void testRemoveAndExpiration() {
        AgeOutCache<Integer> cache = new AgeOutCache<Integer>(timer, 500L);
        for (int i = 1; i <= 5; i++)
            cache.add(i);
        System.out.println("cache:\n" + cache);
        cache.remove(3);
        cache.remove(5);
        cache.remove(6); // not existent
        System.out.println("cache:\n" + cache);
        assert cache.size() == 3 : "cache size should be 3 but is " + cache;
        Util.sleep(1500);
        assert cache.size() == 0;
    }


    public void testExists() {
        AgeOutCache<Integer> cache = new AgeOutCache<Integer>(timer, 5000L);
        for (int i = 1; i <= 5; i++)
            cache.add(i);
        System.out.println("cache:\n" + cache);
        assert cache.exists(3);
        cache.remove(3);
        System.out.println("cache:\n" + cache);
        assert !cache.exists(3);
        cache.clear();
        assert cache.size() == 0;
        assert !cache.exists(4);
    }

    public void testGradualExpiration() {
        AgeOutCache<Integer> cache = new AgeOutCache<Integer>(timer, 5000L,
                                                              new AgeOutCache.Handler<Integer>() {
                                                                  public void expired(Integer key) {
                                                                      System.out.println(key + " expired");
                                                                  }
                                                              });
        for (int i = 1; i <= 10; i++) {
            cache.add(i);
            System.out.print(".");
            Util.sleep(1000);
        }
        System.out.println("\ncache:\n" + cache);
        int size = cache.size();
        assert size < 10 && size > 0 : " size was " + size + ", but should have been < 10 or > 0";
    }
}