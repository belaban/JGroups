// $Id: Profiler.java,v 1.7 2009/05/13 13:07:11 belaban Exp $

package org.jgroups.debug;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Allows to time execution of 'named' statements, counts number of times called and total
 * execution time.
 *
 * @author Bela Ban
 */
public class Profiler {

    static public class Entry {
        long num_calls=0;
        long tot_time=0;
        double avg=0.0;
        long start_time=0;
        long stop_time=0;

        synchronized void compute() {
            num_calls++;
            tot_time+=stop_time - start_time;
            avg=(double)tot_time / num_calls;
        }
    }


    private static OutputStream os=null;
    private static final Hashtable entries=new Hashtable();
    private static Log log=LogFactory.getLog(Profiler.class);


    public Profiler() {
        try {
            os=new FileOutputStream("profiler.dat");
        }
        catch(Exception e) {
            log.error(e.toString());
        }
    }


    public static void setFilename(String filename) {
        try {
            if(os != null) {
                os.close();
            }
            os=new FileOutputStream(filename);
        }
        catch(Exception e) {
            log.error(e.toString());
        }
    }


    public static void start(String call_name) {
        Entry e=(Entry)entries.get(call_name);
        if(e == null) {
            e=new Entry();
            entries.put(call_name, e);
        }
        e.start_time=System.currentTimeMillis();
    }


    public static void stop(String call_name) {
        Entry e=(Entry)entries.get(call_name);
        if(e == null) {
            log.error("Profiler.stop(): entry for " + call_name + " not found");
            return;
        }
        e.stop_time=System.currentTimeMillis();
        e.compute();
    }


    public static void dump() { // dump to file
        String key;
        Entry val;
        if(os == null) {
            log.error("Profiler.dump(): output file is null");
            return;
        }
        try {
            os.write("Key:      Number of calls:    Total time (ms): Average time (ms):\n".getBytes());
            os.write("-----------------------------------------------------------------\n\n".getBytes());
        }
        catch(Exception e) {
            log.error(e.toString());
        }
        for(Enumeration e=entries.keys(); e.hasMoreElements();) {
            key=(String)e.nextElement();
            val=(Entry)entries.get(key);
            try {
                os.write((key + ": " + val.num_calls + ' ' +
                          val.tot_time + ' ' + trim(val.avg) + '\n').getBytes());
            }
            catch(Exception ex) {
                log.error(ex.toString());
            }
        }
    }


    public static double trim(double inp) {
        double retval=0.0, rem=0.0;
        long l1, l2;

        l1=(long)inp;
        rem=inp - l1;
        rem=rem * 100.0;
        l2=(long)rem;
        rem=l2 / 100.0;
        retval=l1 + rem;
        return retval;
    }


    public static void main(String[] args) {
        Profiler.setFilename("bela.out");


        try {

            Profiler.start("time1");
            Thread.sleep(1500);
            Profiler.stop("time1");

            Profiler.start("time1");
            Thread.sleep(1500);
            Profiler.start("time2");
            Thread.sleep(500);

            Profiler.stop("time2");
            Thread.sleep(1500);
            Profiler.stop("time1");


            Profiler.dump();
        }
        catch(Exception e) {
            log.error(e.toString());
        }
    }
}
