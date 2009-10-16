package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MembershipListenerAdapter;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.Date;

/**
 * @author Bela Ban
 * @version $Id: ScaleTest.java,v 1.3 2009/10/16 07:14:47 belaban Exp $
 */
public class ScaleTest {
    JChannel ch;
    String props="udp.xml";
    boolean server=true;
    RpcDispatcher disp;

    static NumberFormat f;

    static {
        f=NumberFormat.getNumberInstance();
        f.setGroupingUsed(false);
        f.setMaximumFractionDigits(2);
    }



    public ScaleTest(String props, boolean server) {
        this.props=props;
        this.server=server;
    }

    public Address getAddress() {
        return ch.getAddress();
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        disp=new RpcDispatcher(ch, null, new MembershipListenerAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("view=" + new_view);
            }
        }, this);
        ch.connect("ScaleTest-Cluster");

        if(!server) {
            loop();
            Util.close(ch);
        }
        else
            System.out.println("ScaleTest started at " + new Date() + ", ready to server requests");
    }

    private void loop() {
        while(true) {
            int input=Util.keyPress(prompt());
            switch(input) {
                case '1':
                    View view=ch.getView();
                    if(view.size() > 10)
                        System.out.println(view.getViewId() + ": " + view.size() + " members");
                    else
                        System.out.println(view + " (" + view.size() + " members)");
                    break;
                case '2':
                    try {
                        invokeRpcs();
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case '3':
                    return;
            }
        }
    }

    private void invokeRpcs() throws Exception {
        Method method=MethodCall.findMethod(ScaleTest.class, "getAddress", null);
        MethodCall call=new MethodCall(method);
        call.setRequestMode(GroupRequest.GET_ALL);
        call.setTimeout(5000);
        call.setFlags(Message.DONT_BUNDLE);
        int num_msgs=Util.readIntFromStdin("Number of RPCs: ");
        int print=num_msgs / 10;
        System.out.println("Invoking " + num_msgs + " RPCs:");
        long start=System.currentTimeMillis();
        for(int i=0; i < num_msgs; i++) {
            disp.callRemoteMethods(null, call);
            if(print > 0 && i % print == 0)
                System.out.println("invoking RPC #" + i);
        }
        long diff=System.currentTimeMillis() - start;
        double rpcs_per_sec=num_msgs / (diff / 1000.0);
        System.out.println("Invoked " + num_msgs + " in " + diff + " ms: " + f.format(rpcs_per_sec) + " RPCs / sec");
    }

    private static String prompt() {
        return "\n[1] View [2] Send RPCs [3] Exit";
    }


    public static void main(String[] args) throws ChannelException {
        String props="udp.xml";
        boolean server=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-server")) {
                server=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        ScaleTest test=new ScaleTest(props, server);
        test.start();
    }

    static void help() {
        System.out.println("ScaleTest [-props properties] [-server (true | false)]");
    }
}
