package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.*;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.TIME_SENSITIVE,singleThreaded=true)
public class RpcDispatcherAsyncInvocationTest {
    protected JChannel            a, b;
    protected RpcDispatcher       disp1, disp2;
    protected final AtomicInteger count=new AtomicInteger(0);
    protected Method              incr_method;

    @Test(enabled=false)
    public int incr() {
        Util.sleep(500);
        return count.incrementAndGet();
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
        disp2.asyncDispatching(true);
        count.set(0);
    }


    /** Tests that 10 sync regular RPCs (each taking 500ms) take roughly 5 seconds */
    public void testRegularInvocation() throws Exception {
        invoke(10, false, 5000, 8000);
    }

    public void testRegularAsyncInvocation() throws Exception {
        invoke(10, false, 5000, 8000);
    }

    public void testOOBAsyncInvocation() throws Exception {
        invoke(10, true, 500, 1000);
    }



    protected void invoke(int num_invocations, boolean use_oob, long expected_min, long expected_max) throws Exception {
        long start=System.currentTimeMillis();
        Collection<Integer> list=invokeRpc(num_invocations, use_oob);
        long time=System.currentTimeMillis() - start;
        System.out.println("took " + time + " ms: " + list);
        assert time >= expected_min && time <= expected_max : // extreme GC could cause this (or even worse)
          "time was expected to be in range [" + expected_min + " .. " + expected_max + "] but was " + time;
    }



    protected Collection<Integer> invokeRpc(final int num_invocations, boolean use_oob) throws Exception {
        RequestOptions opts=RequestOptions.SYNC();
        if(use_oob)
            opts.flags(Message.Flag.OOB);

        final Collection<Integer> results=new ConcurrentLinkedQueue<>();

        MethodCall call=new MethodCall(incr_method);
        for(int i=0; i < num_invocations; i++) {
            CompletableFuture<Object> future=disp1.callRemoteMethodWithFuture(b.getAddress(), call, opts);
            future.whenComplete((result,ex) -> {
                try {
                    results.add((Integer)result);
                    System.out.println("<-- " + result);
                }
                catch(Exception ignored) {}
            });
        }
        Util.waitUntil(10000, 50, () -> results.size() == num_invocations,
                       () -> String.format("expected %d but size is %d: %s", num_invocations, results.size(), results));
        return results;
    }


    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK().setThreadPoolMinThreads(10).setThreadPoolMaxThreads(20),
                            new SHARED_LOOPBACK_PING(),
                            new NAKACK2(),
                            new UNICAST3(),
                            new GMS()).name(name);
    }


    protected class MyRequestHandler implements RequestHandler {
        public void handle(final Message request, final Response response) throws Exception {
            if(request.isFlagSet(Message.Flag.OOB)) {
                new Thread(() -> {
                    int val=incr();
                    if(response != null)
                        response.send(val, false);
                }).start();
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
