package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.*;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.TIME_SENSITIVE,sequential=true)
public class RpcDispatcherAsyncInvocationTest {
    protected JChannel      a, b;
    protected RpcDispatcher disp1, disp2;
    protected int           count=1;
    protected Method        incr_method;

    public int incr() {
        Util.sleep(500);
        return count++;
    }

    @AfterMethod protected void destroy() {disp2.stop(); disp1.stop(); Util.close(b,a);}

    @BeforeMethod
    protected void init() throws Exception {
        incr_method=RpcDispatcherAsyncInvocationTest.class.getMethod("incr");
        a=createChannel("A");
        b=createChannel("B");
        disp1=new RpcDispatcher(a, this);
        disp2=new RpcDispatcher(b, this);
        a.connect("RpcDispatcherAsyncInvocationTest");
        b.connect("RpcDispatcherAsyncInvocationTest");
        disp2.setRequestHandler(new MyRequestHandler());
        count=1;
    }


    /** Tests that 10 sync regular RPCs (each taking 500ms) take roughly 5 seconds */
    public void testRegularInvocation() throws Exception {
        invoke(10, false, false, 5000, 8000);
    }

    public void testRegularAsyncInvocation() throws Exception {
        invoke(10, true, false, 5000, 8000);
    }

    public void testOOBAsyncInvocation() throws Exception {
        invoke(10, true, true, 500, 1000);
    }



    protected void invoke(int num_invocations, boolean async, boolean use_oob, long expected_min, long expected_max) throws Exception {
        long start=System.currentTimeMillis();
        List<Integer> list=invokeRpc(num_invocations, use_oob);
        long time=System.currentTimeMillis() - start;
        System.out.println("took " + time + " ms: " + list);
        assert list.size() == num_invocations;
        assert time >= expected_min && time <= expected_max; // extreme GC could cause this (or even worse)
    }



    protected List<Integer> invokeRpc(final int num_invocations, boolean use_oob) throws Exception {
        RequestOptions opts=RequestOptions.SYNC();
        if(use_oob)
            opts.setFlags(Message.Flag.OOB);

        final List<Integer> results=new ArrayList<Integer>(num_invocations);
        FutureListener<Integer> listener=new FutureListener<Integer>() {
            public void futureDone(Future<Integer> future) {
                try {
                    int result=future.get();
                    results.add(result);
                    System.out.println("<-- " + result);
                    if(results.size() == num_invocations) {
                        synchronized(results) {
                            results.notifyAll();
                        }
                    }
                }
                catch(Exception e) {
                }
            }
        };

        for(int i=0; i < num_invocations; i++) {
            NotifyingFuture<Integer> future=disp1.callRemoteMethodWithFuture(b.getAddress(),new MethodCall(incr_method),opts);
            future.setListener(listener);
        }

        for(int i=0; i < 20; i++) {
            synchronized(results) {
                if(results.size() == num_invocations)
                    break;
                results.wait(500);
            }
        }
        return results;
    }


    protected static JChannel createChannel(String name) throws Exception {
        TP transport=new SHARED_LOOPBACK();
        transport.setOOBThreadPoolMinThreads(10);
        transport.setOOBThreadPoolMaxThreads(20);
        return new JChannel(new Protocol[]{
          transport,
          new PING().setValue("timeout", 500),
          new NAKACK2(),
          new UNICAST(),
          new GMS()
        }).name(name);
    }


    protected class MyRequestHandler implements AsyncRequestHandler {
        public void handle(final Message request, final Response response) throws Exception {
            if(request.isFlagSet(Message.Flag.OOB)) {
                new Thread() {
                    public void run() {
                        int val=incr();
                        if(response != null)
                            response.send(val, false);
                    }
                }.start();
            }
            else {
                int val=incr();
                if(response != null)
                    response.send(val, false);
            }
        }

        public Object handle(Message msg) throws Exception {
            return incr();
        }
    }



}
