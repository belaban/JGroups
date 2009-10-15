package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.util.RspList;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;

/**
 * @author Bela Ban
 * @version $Id: ScaleTest.java,v 1.1 2009/10/15 10:06:01 belaban Exp $
 */
public class ScaleTest {
    JChannel ch;
    String props="udp.xml";
    boolean server=true;
    RpcDispatcher disp;

    public ScaleTest(String props, boolean server) {
        this.props=props;
        this.server=server;
    }

    public Address getAddress() {
        return ch.getAddress();
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        disp=new RpcDispatcher(ch, null, null, this);
        ch.connect("ScaleTest-Cluster");

        if(!server) {
            loop();
            Util.close(ch);
        }
    }

    private void loop() {
        while(true) {
            int input=Util.keyPress(prompt());
            switch(input) {
                case '1':
                    View view=ch.getView();
                    System.out.println(view.getViewId() + ": " + view.size() + " members");
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
        for(int i=0; i < num_msgs; i++) {
            RspList rsps=disp.callRemoteMethods(null, call);
            System.out.println("rsps:\n" + rsps);
        }
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
