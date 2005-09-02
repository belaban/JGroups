package org.jgroups.tests;

//import gnu.trove.TLongObjectHashMap;
//import gnu.trove.THashMap;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


/**
 * @author Bela Ban
 * @version $Id: HashMapTest.java,v 1.5 2005/09/02 14:27:38 belaban Exp $
 */
public class HashMapTest {


    public static void main(String[] args) throws Exception {
        int num=10000;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("HashMapTest [-num <num>] [-help]");
            return;
        }
        HashMapTest t=new HashMapTest();
        Class[] classes=new Class[]{HashMap.class, TreeMap.class, ConcurrentReaderHashMap.class, ConcurrentHashMap.class};
        Map[] maps=new Map[classes.length];

        System.out.println("\nTesting creation times");
        for(int i=0; i < classes.length; i++) {
            t.testCreation(classes[i], num);
        }

        for(int i=0; i < classes.length; i++)
            maps[i]=(Map)classes[i].newInstance();


        System.out.println("\nTesting puts and gets");
        for(int i=0; i < maps.length; i++) {
            t.testPutAndGet(maps[i], num);
        }
    }

   /* private void start3(THashMap m, int num) {
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            m.put(new Long(i), "bla");
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to insert " + m.size() + " elements into " + m.getClass().getName());
        m.clear();
    }

    private void start2(TLongObjectHashMap m, int num) {
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            m.put(i, "bla");
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to insert " + m.size() + " elements into " + m.getClass().getName());
        m.clear();
    }*/

    private void testCreation(Class cl, int num) throws IllegalAccessException, InstantiationException {
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            cl.newInstance();
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to create " + num + " instances of " + cl.getName());
    }


    private void testPutAndGet(Map m, int num) throws Exception {
        long start, stop;
        Object retval;

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            m.put(new Long(i), "bla");
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to insert " + m.size() + " elements into " + m.getClass().getName());

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            retval=m.get(new Long(i));
            if(retval == null)
                throw new Exception("retval for " + i + " is null");
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to fetch " + m.size() + " elements from " + m.getClass().getName());

        start=System.currentTimeMillis();
        m.clear();
        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to clear " + m.getClass().getName() + "\n");
    }


}
