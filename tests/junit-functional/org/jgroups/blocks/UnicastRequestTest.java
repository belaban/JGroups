
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.protocols.DROP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UnicastRequestTest {
    protected Address a, b, c;
    protected static final byte[] data="bla".getBytes();
    protected static final ByteArray buf=new ByteArray(data, 0, data.length);

    @BeforeClass
    void init() throws UnknownHostException {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
    }


    public void testSimpleInvocation() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new BytesMessage(b, (long)322649)}, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        Long result=req.execute(new BytesMessage(a, buf), true);
        System.out.println("result = " + result);
        assert result != null && result == 322649;

        result=req.get(); // use the future
        System.out.println("result = " + result);
        assert result != null && result == 322649;
    }

    public void testSimpleVoidInvocation() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new BytesMessage(b, (String)null)}, 0);
        UnicastRequest<String> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        String result=req.execute(new BytesMessage(a, buf), true);
        assert req.isDone();
        System.out.println("result = " + result);
        assert result == null;

        result=req.get();
        System.out.println("result = " + result);
        assert result == null;
    }

    public void testInvocationWithException() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, new Object[]{new BytesMessage(b, (long)322649)}, 0);
        UnicastRequest<Object> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        req.receiveResponse(new NullPointerException("booom"), b, false);
        Object result=req.execute(new BytesMessage(a, buf), true);
        System.out.println("result = " + result);
        assert result instanceof NullPointerException;

        result=req.get(); // use the future
        System.out.println("result = " + result);
        assert result instanceof NullPointerException;

        req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(1000));
        corr.setRequest(req);
        req.receiveResponse(new NullPointerException("booom"), b, true);

        try {
            req.execute(new BytesMessage(a, buf), true);
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
            req.execute(new BytesMessage(a, buf), true);
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
        req.viewChange(new_view, false);

        try {
            req.execute(new BytesMessage(a, buf), true);
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

    public void testMissingResponseDueToMergeViewUnicast() throws Exception {
        try(JChannel ch1=create("A"); JChannel ch2=create("B");
            MessageDispatcher md1=new MessageDispatcher(ch1, r -> "from A");
            MessageDispatcher ignored=new MessageDispatcher(ch2, r -> "from B")) {
            Util.waitUntilAllChannelsHaveSameView(10000, 500, ch1,ch2);

            Address a_addr=ch1.getAddress(), b_addr=ch2.getAddress();
            long view_id=ch1.getView().getViewId().getId();

            // the DROP protocol drops the unicast response from B -> A
            DROP drop=new DROP().addDownFilter(m -> Objects.equals(m.getDest(), a_addr));
            ch2.getProtocolStack().insertProtocolAtTop(drop);

            Message msg=new ObjectMessage(b_addr, "req from A");
            CompletableFuture<Object> f=md1.sendMessageWithFuture(msg, RequestOptions.SYNC());

            // inject view {B} into B; this causes B to drop connections to A and remove A from B's retransmission tables
            View v=View.create(b_addr,  ++view_id, b_addr),
              mv=new MergeView(a_addr, ++view_id, Arrays.asList(a_addr, b_addr), Arrays.asList(ch1.getView(), v));
            GMS gms=ch2.getProtocolStack().findProtocol(GMS.class);
            gms.installView(v);
            assert ch2.getView().size() == 1;

            // now inject the MergeView in B and A
            gms.installView(mv);
            GMS tmp=ch1.getProtocolStack().findProtocol(GMS.class);
            tmp.installView(mv);

            try {
                f.get(5, TimeUnit.SECONDS);
                assert false: "should have thrown a SuspectedException";
            }
            catch(ExecutionException ex) {
                if(ex.getCause() instanceof SuspectedException)
                    System.out.printf("received exception as expected: %s\n", ex);
                else throw ex;
            }
        }
    }

    public void testCancel() throws Exception {
        MyCorrelator corr=new MyCorrelator(false, null, 0);
        UnicastRequest<Long> req=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(5000));
        corr.setRequest(req);
        req.cancel(true);

        try {
            req.execute(new BytesMessage(a, buf), true);
            assert false : "should have thrown CancellationException";
        }
        catch(CancellationException ex) {
            System.out.printf("received %s as expected\n", ex);
        }

        final UnicastRequest<Long> req2=new UnicastRequest<>(corr, a, RequestOptions.SYNC().timeout(5000));
        corr.setRequest(req2);

        new Thread(() -> {Util.sleep(1000); req2.cancel(true);}).start();

        try {
            req2.execute(new BytesMessage(a, buf), true);
            assert false : "should have thrown CancellationException";
        }
        catch(CancellationException ex) {
            System.out.printf("received %s as expected\n", ex);
        }
        assert req.isCancelled();
        assert req2.isCancelled();

    }


    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack()).name(name).connect("demo");
    }

    protected static class MyCorrelator extends RequestCorrelator {
        protected UnicastRequest<?> request;
        protected boolean           async=true;
        protected Object[]          responses;
        protected long              delay;

        public MyCorrelator(boolean async, Object[] responses, long delay) {
            super(null, null, null);
            this.async=async;
            this.responses=responses;
            this.delay=delay;
            this.down_prot=new Protocol() {
                public Object down(Event evt) {
                    return null;
                }
            };
        }

        public void setRequest(UnicastRequest<?> r) {
            request=r;
        }


        @Override
        public <T> void sendUnicastRequest(Message msg, Request<T> req, RequestOptions opts) throws Exception {
            send();
        }

        protected void send() {
            if(async) {
                new Thread(this::sendResponses).start();
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
                            retval=Util.objectFromByteBuffer(msg.getArray(), msg.getOffset(), msg.getLength());
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                        request.receiveResponse(retval, sender, false);
                    }
                    else if(obj instanceof View)
                        request.viewChange((View)obj, false);
                }
            }
        }
    }

}
