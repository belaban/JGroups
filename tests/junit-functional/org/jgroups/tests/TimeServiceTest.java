package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.TimeService;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests {@TimeService}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TimeServiceTest {
    protected TimeScheduler timer;
    protected TimeService   time_service;

    @BeforeClass  public void init()    {timer=new TimeScheduler3();}
    @BeforeMethod public void start()   {time_service=new TimeService(timer).start();}
    @AfterMethod  public void stop()    {time_service.stop();}
    @AfterClass   public void destroy() {timer.stop();}


    public void testSimpleGetTime() {
        List<Long> times=new ArrayList<>(20);
        for(int i=0; i < 20; i++)
            times.add(time_service.timestamp());

        System.out.println("times=" + times);

        Set<Long> set=new HashSet<>(times);
        System.out.println("set = " + set);

        assert set.size() < times.size();
        assert times.size() <= 20;

        set.clear();
        time_service.stop().interval(50).start();
        for(int i=0; i < 20; i++) {
            set.add(time_service.timestamp());
            Util.sleep(200);
        }

        System.out.println("set=" + set);

        assert set.size() >= 15;
    }


    public void testChangeInterval() {
        time_service.interval(1000).start();
        assert time_service.interval() == 1000;
    }

    public void testStartStop() {
        assert time_service.running();
        time_service.stop();
        Util.sleep(2000);
        assert !time_service.running();
    }

}
