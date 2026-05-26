package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;


/**
 * Test for measuring performance of RPCs. See {@link RoundTrip} for simple messages
 * @author Bela Ban
 */
public class RpcDispatcherSpeedTest implements Receiver {
    protected JChannel              channel;
    protected RpcDispatcher         disp;
    protected String                props;
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


    public void start(String props, String name) throws Exception {
        channel=new JChannel(props).name(name);
        disp=new RpcDispatcher(channel, this).setReceiver(this).setMethodLookup(id -> METHODS[0]);
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
        AverageMinMax avg=new AverageMinMax(1024).unit(TimeUnit.NANOSECONDS);
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
            long time=System.nanoTime() - start;
            if(i > 0 && i % print == 0)
                System.out.print(".");
            boolean all_received=rsps.values().stream().allMatch(Rsp::wasReceived);
            if(!all_received)
                System.err.printf("didn't receive all responses: %s\n", rsps);
            avg.add(time);
        }
        System.out.printf("\\nnround-trip = %s\n\n", avg);
    }





    public void viewAccepted(View new_view) {
        System.out.println("-- new view: " + new_view);
    }


    public static void main(String[] args) throws Exception {
        String                 props=null, name=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
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
        test.start(props, name);
    }

    static void help() {
        System.out.println("RpcDispatcherSpeedTest [-help] [-props <props>] [-name name]");
    }
}
