package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.lang.reflect.Method;




/**
 * Interactive test for measuring group RPCs using different invocation techniques.
 * @author Bela Ban
 * @version $Revision: 1.17 $
 */
public class RpcDispatcherSpeedTest implements MembershipListener {
    Channel             channel;
    RpcDispatcher       disp;
    String              props=null;
    boolean             server=false; // role is client by default
    boolean             jmx=false;
    int                 num=1000;
    int                 mode=OLD;
    static final int    OLD=1;
    static final int    METHOD=2;
    static final int    TYPES=3;
    static final int    SIGNATURE=4;
    static final int    ID=5;
    static final long   TIMEOUT=10000;
    static final Class  LONG_CLASS=long.class;
    static final String LONG=long.class.getName();
    final Method[]      METHODS=new Method[1];
    final Object[]      EMPTY_OBJECT_ARRAY=new Object[]{};
    final Class[]       EMPTY_CLASS_ARRAY=new Class[]{};
    final String[]      EMPTY_STRING_ARRAY=new String[]{};
    private long        sleep=0;
    private boolean     async;


    public RpcDispatcherSpeedTest(String props, boolean server, boolean async, int num, int mode, boolean jmx, long sleep) throws NoSuchMethodException {
        this.props=props;
        this.server=server;
        this.async=async;
        this.num=num;
        this.mode=mode;
        this.jmx=jmx;
        this.sleep=sleep;
        initMethods();
    }

    final void initMethods() throws NoSuchMethodException {
        Class cl=this.getClass();
        METHODS[0]=cl.getMethod("measure", (Class[])null);
    }

    public long measure() throws Exception {
        long retval=System.currentTimeMillis();
        if(sleep > 0)
            Util.sleep(sleep);
        return retval;
    }


    public void start() throws Exception {
        channel=new JChannel(props);
        channel.setOpt(Channel.LOCAL, Boolean.FALSE);
        disp=new RpcDispatcher(channel, null, this, this,
                false, // no deadlock detection
                false); // no concurrent processing on incoming method calls
        // disp.setConcurrentProcessing(true);

        disp.setMethodLookup(new MethodLookup() {

            public Method findMethod(short id) {
                return METHODS[0];
            }
        });


        if(jmx) {
            MBeanServer srv=Util.getMBeanServer();
            if(srv == null)
                throw new Exception("No MBeanServers found;" +
                        "\nDraw needs to be run with an MBeanServer present, or inside JDK 5");
            JmxConfigurator.registerChannel((JChannel)channel, srv, "jgroups", channel.getClusterName(), true);
        }

        channel.connect("RpcDispatcherSpeedTestGroup");

        try {
            if(server) {
                System.out.println("-- Started as server. Press ctrl-c to kill");
                while(true) {
                    Util.sleep(10000);
                }
            }
            else {
                invokeRpcs(num, mode, async);
            }
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
        }
        finally {
            channel.close();
            disp.stop();
        }
    }


    void invokeRpcs(int num, int mode, boolean async) throws Exception {
        long    start, stop;
        int     show=num/10;
        Method measure_method=getClass().getMethod("measure", EMPTY_CLASS_ARRAY);
        MethodCall measure_method_call=new MethodCall(measure_method, EMPTY_OBJECT_ARRAY);

        if(show <=0)
            show=1;
        int request_type=async ? GroupRequest.GET_NONE : GroupRequest.GET_ALL;
        start=System.currentTimeMillis();
        switch(mode) {
        case OLD:
            System.out.println("-- invoking " + num + " methods using mode=OLD");
            for(int i=1; i <= num; i++) {
                disp.callRemoteMethods(null,
                                       "measure",
                                       EMPTY_OBJECT_ARRAY,
                                       EMPTY_CLASS_ARRAY,
                                       request_type, TIMEOUT);
                if(i % show == 0)
                    System.out.println(i);
            }
            break;

        case METHOD:
            System.out.println("-- invoking " + num + " methods using mode=METHOD");
            for(int i=1; i <= num; i++) {
                disp.callRemoteMethods(null, measure_method_call, request_type, TIMEOUT);
                if(i % show == 0)
                    System.out.println(i);
            }
            break;

        case TYPES:
            System.out.println("-- invoking " + num + " methods using mode=TYPES");
            for(int i=1; i <= num; i++) {
                disp.callRemoteMethods(null, "measure",
                                       EMPTY_OBJECT_ARRAY,
                                       EMPTY_CLASS_ARRAY,
                                       request_type,
                                       TIMEOUT);
                if(i % show == 0)
                    System.out.println(i);
            }
            break;

        case SIGNATURE:
            System.out.println("-- invoking " + num + " methods using mode=SIGNATURE");
            for(int i=1; i <= num; i++) {
                disp.callRemoteMethods(null, "measure",
                                       EMPTY_OBJECT_ARRAY,
                                       EMPTY_STRING_ARRAY,
                                       request_type,
                                       TIMEOUT);
                if(i % show == 0)
                    System.out.println(i);
            }
            break;
        case ID:
            System.out.println("-- invoking " + num + " methods using mode=ID");
            measure_method_call=new MethodCall((short)0, null);
            measure_method_call.setRequestMode(request_type);
            measure_method_call.setTimeout(TIMEOUT);
            measure_method_call.setUseAnycasting(false);
            measure_method_call.setFlags(Message.DONT_BUNDLE);
            measure_method_call.setFilter(null);
            for(int i=1; i <= num; i++) {
                disp.callRemoteMethods(null, measure_method_call);
                if(i % show == 0)
                    System.out.println(i);
            }
            break;
        default:
            break;
        }
        stop=System.currentTimeMillis();
        printStats(stop-start, num);
    }



    static void printStats(long total_time, int num) {
        double throughput=((double)num)/((double)total_time/1000.0);
        System.out.println("time for " + num + " remote calls was " +
                           total_time + ", avg=" + (total_time / (double)num) +
                           "ms/invocation, " + (long)throughput + " calls/sec");
    }

    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }



    public void suspect(Address suspected_mbr) {
        ;
    }



    public void block() {
        ;
    }



    public static void main(String[] args) {
        String                 props=null;
        boolean                server=false, jmx=false;
        int                    num=1000;
        long                   sleep=0;
        RpcDispatcherSpeedTest test;
        int                    mode=OLD;
        boolean                async=false;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-server".equals(args[i])) {
                server=true;
                continue;
            }
            if("-async".equals(args[i])) {
                async=true;
                continue;
            }
            if("-num".equals(args[i])) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            if("-sleep".equals(args[i])) {
                sleep=Long.parseLong(args[++i]);
                continue;
            }
            if("-mode".equals(args[i])) {
                String m=args[++i].toLowerCase().trim();
                if("old".equals(m))
                    mode=OLD;
                else if("method".equals(m))
                    mode=METHOD;
                else if("types".equals(m))
                    mode=TYPES;
                else if("signature".equals(m))
                    mode=SIGNATURE;
                else if("ID".equals(m) || "id".equals(m)) {
                    mode=ID;
                }
                else {
                    System.err.println("mode " + m + " is invalid");
                    help();
                    return;
                }
                continue;
            }
            help();
            return;
        }


        try {
            test=new RpcDispatcherSpeedTest(props, server, async, num, mode, jmx, sleep);
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] " +
                           "[-server] [-async] [-num <number of calls>] [-mode <mode>] [-jmx] [-sleep <ms>]");
        System.out.println("mode can be either 'old', 'method', 'types', signature' or 'id'");
    }
}
