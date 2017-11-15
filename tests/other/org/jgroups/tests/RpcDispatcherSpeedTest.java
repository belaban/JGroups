package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Average;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.lang.reflect.Method;


/**
 * Test for measuring performance of RPCs. See {@link RoundTrip} for simple messages
 * @author Bela Ban
 */
public class RpcDispatcherSpeedTest implements MembershipListener {
    protected JChannel              channel;
    protected RpcDispatcher         disp;
    protected String                props;
    protected boolean               jmx;
    protected int                   num=5000;
    protected static final Method[] METHODS=new Method[1];
    protected boolean               oob, dont_bundle;
    protected static final String   format="[1] invoke RPCs [2] num (%d) [o] oob (%b) [b] dont_bundle (%b) [x] exit\n";


    static {
        try {
            METHODS[0]=RpcDispatcherSpeedTest.class.getMethod("measure");
        }
        catch(Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void measure() {
        ;
    }


    public void start(String props, boolean jmx, String name) throws Exception {
        channel=new JChannel(props).name(name);
        disp=new RpcDispatcher(channel, this) // no concurrent processing on incoming method calls
          .setMembershipListener(this).setMethodLookup(id -> METHODS[0]);

        if(jmx) {
            MBeanServer srv=Util.getMBeanServer();
            if(srv == null)
                throw new Exception("No MBeanServers found");
            JmxConfigurator.registerChannel(channel, srv, "jgroups", channel.getClusterName(), true);
        }
        channel.connect("rpc-speed-test");
        View view=channel.getView();
        if(view.size() > 2)
            System.err.printf("More than 2 members in cluster: %s; terminating\n", view);
        else
            loop();
        Util.close(disp, channel);
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            try {
                int c=Util.keyPress(String.format(format, num, oob, dont_bundle));
                switch(c) {
                    case '1':
                        invokeRpcs();
                        break;
                    case '2':
                        num=Util.readIntFromStdin("num: ");
                        break;
                    case 'o':
                        oob=!oob;
                        break;
                    case 'b':
                        dont_bundle=!dont_bundle;
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected void invokeRpcs() throws Exception {
        Average avg=new Average();
        long min=Long.MAX_VALUE, max=0;
        RequestOptions opts=new RequestOptions(ResponseMode.GET_FIRST, 0).transientFlags(Message.TransientFlag.DONT_LOOPBACK);
        MethodCall call=new MethodCall((short)0);
        int print=num/10;

        if(oob)
            opts.flags(Message.Flag.OOB);
        if(dont_bundle)
            opts.flags(Message.Flag.DONT_BUNDLE);

        if(channel.getView().size() != 2) {
            System.err.printf("Cluster must have exactly 2 members: %s\n", channel.getView());
            return;
        }

        System.out.printf("\nInvoking %d blocking RPCs (oob: %b, dont_bundle: %b)\n", num, oob, dont_bundle);
        for(int i=0; i < num; i++) {
            long start=System.nanoTime();
            RspList<Void> rsps=disp.callRemoteMethods(null, call, opts);
            long time_ns=System.nanoTime() - start;
            if(i > 0 && i % print == 0)
                System.out.print(".");
            boolean all_received=rsps.values().stream().allMatch(Rsp::wasReceived);
            if(!all_received)
                System.err.printf("didn't receive all responses: %s\n", rsps);
            avg.add(time_ns);
            min=Math.min(min, time_ns);
            max=Math.max(max, time_ns);
        }
        System.out.println("");
        System.out.printf("\nround-trip = min/avg/max: %.2f / %.2f / %.2f us\n\n", min/1000.0, avg.getAverage() / 1000.0, max/1000.0);
    }





    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }


    public static void main(String[] args) throws Exception {
        String                 props=null, name=null;
        boolean                jmx=false;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        RpcDispatcherSpeedTest test=new RpcDispatcherSpeedTest();
        test.start(props, jmx, name);
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] [-name name] [-jmx]");
    }
}
