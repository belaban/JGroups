package org.jgroups.tests;

//import gnu.trove.TLongObjectHashMap;
//import gnu.trove.THashMap;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Bela Ban
 * @version $Id: HashMapTest.java,v 1.2 2005/07/13 06:43:35 belaban Exp $
 */
public class HashMapTest {


    public static void main(String[] args) {
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
        t.start(new HashMap(), num);
        t.start(new TreeMap(), num);
        //t.start2(new TLongObjectHashMap(), num);
        //t.start3(new THashMap(), num);
        System.out.println("");
        t.start(new HashMap(), num);
        t.start(new TreeMap(), num);


        System.out.println("");
        t.start(new HashMap(), num);
        t.start(new ConcurrentReaderHashMap(), num);


        System.out.println("");
        t.start(new HashMap(), num);
        t.start(new ConcurrentHashMap(), num);
        //t.start2(new TLongObjectHashMap(), num);
        //t.start3(new THashMap(), num);
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

    private void start(Map m, int num) {
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num; i++) {
            m.put(new Long(i), "bla");
        }

        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop-start) + "ms to insert " + m.size() + " elements into " + m.getClass().getName());
        m.clear();
    }
}
