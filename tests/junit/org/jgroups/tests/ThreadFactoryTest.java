package org.jgroups.tests;

import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.Global;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @version $Id: ThreadFactoryTest.java,v 1.1 2008/05/15 13:18:39 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class ThreadFactoryTest {
    private DefaultThreadFactory factory;
    private static final String BASE="base";
    private static final String ADDR="192.168.1.5:12345";
    private static final String CLUSTER="MyCluster";

    public void testNoNumbering() {
        factory=new DefaultThreadFactory(null, BASE, true, false);
        Thread thread=factory.newThread(new MyRunnable(), BASE);
        String name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals(BASE);
    }

    public void testNumbering() {
        factory=new DefaultThreadFactory(null, BASE, true, true);
        Thread thread=factory.newThread(new MyRunnable(), BASE);
        String name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals("base-1");

        thread=factory.newThread(new MyRunnable(), BASE);
        name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals("base-2");
    }

    public void testPatterns() {
        factory=new DefaultThreadFactory(null, BASE, true, false);
        factory.setAddress(ADDR);
        Thread thread=factory.newThread(new MyRunnable(), BASE);
        String name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals(BASE);

        factory.setPattern("l");
        thread=factory.newThread(new MyRunnable(), BASE);
        name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals(BASE + "," + ADDR);

        factory.setPattern("cl");
        factory.setClusterName(CLUSTER);
        thread=factory.newThread(new MyRunnable(), BASE);
        name=thread.getName();
        System.out.println("name = " + name);
        assert name.equals(BASE + "," + CLUSTER + "," + ADDR);
    }

    static class MyRunnable implements Runnable {
        public void run() {}
    }
}
