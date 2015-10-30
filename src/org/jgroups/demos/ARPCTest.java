package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;



public class ARPCTest {

    JChannel           channel;

    RpcDispatcher      disp;

    RspList            rsp_list;

    String             props = "D:/udp/udp.xml"; // set by application


    public static int print(int number) throws Exception {

        return number * 2;

    }


    public void start() throws Exception {

        MethodCall call=new MethodCall(getClass().getMethod("print", int.class));

        RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
        System.out.println("Props = "+ props);
        channel=new JChannel(props);

        disp=new RpcDispatcher(channel, this);

        channel.connect("RpcDispatcherTestGroup");


        for(int i=0; i < 10; i++) {

            Util.sleep(100);

            rsp_list=disp.callRemoteMethods(null,

                                            "print",

                                            new Object[]{i},

                                            new Class[]{int.class},

                                            opts);

            // Alternative: use a (prefabricated) MethodCall:

            // call.setArgs(i);

            // rsp_list=disp.callRemoteMethods(null, call, opts);

            System.out.println("Responses: " + rsp_list);

        }

        channel.close();

        disp.stop();

    }


    public static void main(String[] args) throws Exception {

        new ARPCTest().start();

    }

}

     