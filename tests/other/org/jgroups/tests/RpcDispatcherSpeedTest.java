package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.MembershipListener;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.RspList;
import org.jgroups.util.Rsp;
import org.jgroups.util.Util;
import org.jgroups.log.Trace;

import java.lang.reflect.Method;




/**
 * Interactive test for measuring group RPCs using different invocation techniques.
 * @author Bela Ban
 * @version $Revision: 1.2 $
 */
public class RpcDispatcherSpeedTest implements MembershipListener {
    Channel             channel;
    RpcDispatcher       disp;
    String              props=null;
    boolean             server=false; // role is client by default
    int                 num=1000;
    int                 mode=OLD;
    static final int    OLD=1;
    static final int    METHOD=2;
    static final int    TYPES=3;
    static final int    SIGNATURE=4;
    static final long   TIMEOUT=10000;
    static final Class  LONG_CLASS=long.class;
    static final String LONG=long.class.getName();



    public RpcDispatcherSpeedTest(String props, boolean server, int num, int mode) {
        this.props=props;
        this.server=server;
        this.num=num;
        this.mode=mode;
    }

    public long measure(long start_time) throws Exception {
        return System.currentTimeMillis() - start_time;
    }


    public void start() throws Exception {
        channel=new JChannel(props);
        disp=new RpcDispatcher(channel, null, this, this);
        channel.connect("RpcDispatcherSpeedTestGroup");

        try {
            if(server) {
                System.out.println("-- Started as server. Press ctrl-c to kill");
                while(true) {
                    Util.sleep(10000);
                }
            }
            else {
                invokeRpcs(num, mode);
            }
        }
        catch(Throwable t) {
            t.printStackTrace(System.err);
        }
        finally {
            System.out.println("Closing channel");
            channel.close();
            System.out.println("Closing channel: -- done");

            System.out.println("Stopping dispatcher");
            disp.stop();
            System.out.println("Stopping dispatcher: -- done");
        }
    }


    void invokeRpcs(int num, int mode) throws Exception {
        RspList rsp_list;
        Long    start_time;
        long    total_time=0;
        int     show=num/10;

        if(show <=0) show=1;
        switch(mode) {
            case OLD:
                System.out.println("-- invoking " + num + " methods using mode=OLD");
                for(int i=1; i <= num; i++) {
                    start_time=new Long(System.currentTimeMillis());
                    rsp_list=disp.callRemoteMethods(null,
                                                    "measure",
                                                    start_time,
                                                    GroupRequest.GET_ALL, TIMEOUT);
                    total_time+=getAverage(rsp_list);
                    if(i % show == 0)
                        System.out.println(i);
                }
                printStats(total_time, num);
                break;

            case METHOD:
                System.out.println("-- invoking " + num + " methods using mode=METHOD");
                Method method=getClass().getMethod("measure", new Class[]{long.class});
                MethodCall method_call;
                for(int i=1; i <= num; i++) {
                    start_time=new Long(System.currentTimeMillis());
                    method_call=new MethodCall(method, new Object[]{start_time});
                    rsp_list=disp.callRemoteMethods(null, method_call, GroupRequest.GET_ALL,
                                                    TIMEOUT);
                    total_time+=getAverage(rsp_list);
                    if(i % show == 0)
                        System.out.println(i);
                }
                printStats(total_time, num);
                break;

                case TYPES:
                System.out.println("-- invoking " + num + " methods using mode=TYPES");
                for(int i=1; i <= num; i++) {
                    start_time=new Long(System.currentTimeMillis());
                    rsp_list=disp.callRemoteMethods(null, "measure",
                                                    new Object[]{start_time},
                                                    new Class[]{LONG_CLASS},
                                                    GroupRequest.GET_ALL,
                                                    TIMEOUT);
                    total_time+=getAverage(rsp_list);
                    if(i % show == 0)
                        System.out.println(i);
                }
                printStats(total_time, num);
                break;

            case SIGNATURE:
                System.out.println("-- invoking " + num + " methods using mode=SIGNATURE");
                for(int i=1; i <= num; i++) {
                    start_time=new Long(System.currentTimeMillis());
                    rsp_list=disp.callRemoteMethods(null, "measure",
                                                    new Object[]{start_time},
                                                    new String[]{LONG},
                                                    GroupRequest.GET_ALL,
                                                    TIMEOUT);
                    total_time+=getAverage(rsp_list);
                    if(i % show == 0)
                        System.out.println(i);
                }
                printStats(total_time, num);
                break;
            default:
                break;
        }

    }


    double getAverage(RspList rsps) {
        Rsp    rsp;
        double retval=0;
        int    num=0;

        if(rsps == null || rsps.size() == 0) {
            System.err.println("response list is empty");
            return 0.0;
        }
        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.getValue() != null && rsp.getValue() instanceof Long) {
                retval+=((Long)rsp.getValue()).longValue();
                num++;
            }
            else {
                System.err.println("response " + rsp.getValue() + " invalid");
            }
        }
        return retval / num;
    }

    void printStats(long total_time, int num) {
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
        boolean                server=false;
        int                    num=1000;
        RpcDispatcherSpeedTest test;
        int                    mode=OLD;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-server")) {
                server=true;
                continue;
            }
            if(args[i].equals("-num")) {
                num=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-mode")) {
                String m=args[++i].toLowerCase().trim();
                if(m.equals("old"))
                    mode=OLD;
                else if(m.equals("method"))
                    mode=METHOD;
                else if(m.equals("types"))
                    mode=TYPES;
                else if(m.equals("signature"))
                    mode=SIGNATURE;
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

        Trace.init();
        try {
            test=new RpcDispatcherSpeedTest(props, server, num, mode);
            test.start();
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] " +
                           "[-server] [-num <number of calls>] [-mode <mode>]");
        System.out.println("mode can be either 'old', 'method', 'types' or 'signature'");
    }
}
