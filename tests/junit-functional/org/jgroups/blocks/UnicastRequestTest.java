
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Buffer;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UnicastRequestTest {
    protected Address a, b, c;
    protected static final byte[] data="bla".getBytes();
    protected static final Buffer buf=new Buffer(data, 0, data.length);

    @BeforeClass
    void init() throws UnknownHostException {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
    }


    public void testSimpleInvocation() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new Message(b, (long)322649)}, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        Long result=req.execute(buf, true);
        System.out.println("result = " + result);
        assert result != null && result == 322649;

        result=req.get(); // use the future
        System.out.println("result = " + result);
        assert result != null && result == 322649;
    }

    public void testSimpleVoidInvocation() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new Message(b, (String)null)}, 0);
        UnicastRequest<String> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        String result=req.execute(buf, true);
        assert req.isDone();
        System.out.println("result = " + result);
        assert result == null;

        result=req.get();
        System.out.println("result = " + result);
        assert result == null;
    }

    public void testInvocationWithException() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new Message(b, (long)322649)}, 0);
        UnicastRequest<Object> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        req.receiveResponse(new NullPointerException("booom"), b, false);
        Object result=req.execute(buf, true);
        System.out.println("result = " + result);
        assert result != null && result instanceof NullPointerException;

        result=req.get(); // use the future
        System.out.println("result = " + result);
        assert result != null && result instanceof NullPointerException;

        req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        req.receiveResponse(new NullPointerException("booom"), b, true);

        try {
            req.execute(buf, true);
            assert false : "should have thrown NullPointerException";
        }
        catch(NullPointerException ex) {
            System.out.printf("received %s as expected\n", ex);
        }

        try {
            req.get(); // use the future
            assert false : "should have thrown ExecutionException";
        }
        catch(ExecutionException ex) {
            System.out.printf("received %s as expected\n", ex);
            assert ex.getCause() instanceof NullPointerException;
        }
    }
    

    public void testInvocationWithTimeout() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, null, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(500));
        corr.setRequest(req);

        try {
            req.execute(buf, true);
            assert false : "should have thrown TimeoutException";
        }
        catch(TimeoutException ex) {
            System.out.printf("received %s as expected\n", ex);
        }

        try {
            req.get(500, TimeUnit.MILLISECONDS); // use the future
            assert false : "should have thrown ExecutionException";
        }
        catch(TimeoutException ex) {
            System.out.printf("received %s as expected\n", ex);
        }
    }


    public void testViewChange() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, null, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(5000));
        corr.setRequest(req);
        View new_view=View.create(b, 5, b,c);
        req.viewChange(new_view);

        try {
            req.execute(buf, true);
        }
        catch(SuspectedException ex) {
            System.out.printf("received %s as expected\n", ex);
        }

        try {
            req.get(100, TimeUnit.MILLISECONDS);
            assert false : "should have thrown ExecutionException";
        }
        catch(ExecutionException ex) {
            System.out.printf("received %s as expected\n", ex);
            assert ex.getCause() instanceof SuspectedException;
        }
    }

    public void testCancel() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, null, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(5000));
        corr.setRequest(req);
        req.cancel(true);

        try {
            req.execute(buf, true);
            assert false : "should have thrown CancellationException";
        }
        catch(CancellationException ex) {
            System.out.printf("received %s as expected\n", ex);
        }

        final UnicastRequest<Long> req2=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(5000));
        corr.setRequest(req2);

        new Thread(() -> {Util.sleep(1000); req2.cancel(true);}).start();

        try {
            req2.execute(buf, true);
            assert false : "should have thrown CancellationException";
        }
        catch(CancellationException ex) {
            System.out.printf("received %s as expected\n", ex);
        }
        assert req.isCancelled();
        assert req2.isCancelled();

    }



    protected static class MyCorrelator extends RequestCorrelator {
        protected UnicastRequest request;
        protected boolean        async=true;
        protected Object[]       responses;
        protected long           delay;

        public MyCorrelator(boolean async, Object[] responses, long delay) {
            super(null, null, null);
            this.async=async;
            this.responses=responses;
            this.delay=delay;
            this.transport=new Protocol() {
                public Object down(Event evt) {
                    return null;
                }
            };
        }

        public void setRequest(UnicastRequest r) {
            request=r;
        }


        @Override
        public void sendUnicastRequest(Address dest, Buffer data, Request req, RequestOptions opts) throws Exception {
            send();
        }

        protected void send() {
            if(async) {
                new Thread() {
                    public void run() {
                        sendResponses();
                    }
                }.start();
            }
            else {
                sendResponses();
            }
        }

        protected void sendResponses() {
            if(responses != null) {
                Object obj;
                for(int i=0; i < responses.length; i++) {
                    if(delay > 0)
                        Util.sleep(delay);
                    obj=responses[i];
                    if(obj == null) {
                        System.err.println("object was null");
                        continue;
                    }
                    if(obj instanceof Message) {
                        Message msg=(Message)obj;
                        Address sender=msg.getSrc();
                        Object retval=null;
                        try {
                            retval=Util.objectFromByteBuffer(msg.getBuffer());
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                        request.receiveResponse(retval, sender, false);
                    }
                    else if(obj instanceof View)
                        request.viewChange((View)obj);
                }
            }
        }
    }

}
