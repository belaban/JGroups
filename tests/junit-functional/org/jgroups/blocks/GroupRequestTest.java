
package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.protocols.DROP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Buffer;
import org.jgroups.util.RspList;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class GroupRequestTest {
    protected Address             a, b, c;
    protected List<Address>       dests;
    protected static final byte[] buf="bla".getBytes();

    @BeforeClass
    void init() throws UnknownHostException {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
    }

    @BeforeMethod
    protected void setUp() throws Exception {
        dests=new ArrayList<>(Arrays.asList(a,b));
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        dests.clear();
    }


    

    public void testMessageTimeout() throws Exception {
        _testMessageTimeout(true);
    }

    public void testMessageReception() throws Exception {
        _testMessageReception(true);
        _testMessageReception(false);
    }


    public void testMessageReceptionWithViewChange() throws Exception {
        _testMessageReceptionWithViewChange(true);
        _testMessageReceptionWithViewChange(false);
    }

    public void testMessageReceptionWithViewChangeMemberLeft() throws Exception {
        _testMessageReceptionWithViewChangeMemberLeft(true);
        _testMessageReceptionWithViewChangeMemberLeft(false);
    }


    public void testGetFirstWithResponseFilter() throws Exception {
        Object[] responses={new Message(null,(long)1).src(a),
          new Message(null,(long)2).src(b),
          new Message(null,(long)3).src(c)};
        MyCorrelator corr=new MyCorrelator(true, responses, 500);
        dests.add(c);
        GroupRequest<Long> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_FIRST, 0));
        req.setResponseFilter(new RspFilter() {
            int num_rsps=0;

            public boolean isAcceptable(Object response, Address sender) {
                boolean retval=response instanceof Long && (Long)response == 2L;
                System.out.println("-- received " + response + " from " + sender + ": " + (retval? "OK" : "NOTOK"));
                if(retval)
                    num_rsps++;
                return retval;
            }

            public boolean needMoreResponses() {
                return num_rsps < 1;
            }
        });
        corr.setGroupRequest(req);

        System.out.println("group request is " + req);
        RspList<Long> results=req.execute(new Buffer(buf, 0, buf.length), true);
        assert req.isDone();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(1, results.numReceived());
    }


    public void testGetAllWithResponseFilter() throws Exception {
        Object[] responses={new Message(null,(long)1).src(a),
          new Message(null,(long)2).src(b),
          new Message(null,(long)3).src(c)};
        MyCorrelator corr=new MyCorrelator(true, responses, 500);
        dests.add(c);
        GroupRequest<Long> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
        req.setResponseFilter(new RspFilter() {
            int num_rsps=0;

            public boolean isAcceptable(Object response, Address sender) {
                boolean retval=response instanceof Long &&
                        ((Long)response == 1L || (Long)response == 2L);
                System.out.println("-- received " + response + " from " + sender + ": " + (retval? "OK" : "NOTOK"));
                if(retval)
                    num_rsps++;
                return retval;
            }

            public boolean needMoreResponses() {
                return num_rsps < 2;
            }
        });
        corr.setGroupRequest(req);
        System.out.println("group request is " + req);
        RspList<Long> results=req.execute(new Buffer(buf, 0, buf.length), true);
        assert req.isDone();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(2, results.numReceived());
    }

    /**
     * Tests reception of 3 null values, which are all rejected by the NonNullFilter. However, isDone() returns true
     * because we received responses for all 3 requests, even though all of them were rejected. If we continued here,
     * we'd block until we run into the timeout. See https://issues.jboss.org/browse/JGRP-1330 for details.
     */
    public void testAllNullResponsesWithFilter() {
        dests.add(c);
        GroupRequest<Boolean> req=new GroupRequest<>(null, dests, RequestOptions.SYNC());
        assert !req.isDone();

        req.setResponseFilter(new NonNullFilter());

        for(Address sender: dests)
            req.receiveResponse(null, sender, false);

        assert req.isDone();
    }
    

    public void testAllNullResponsesWithFilterGetFirst() {
        dests.add(c);
        GroupRequest<Boolean> req=new GroupRequest<>(null, dests, new RequestOptions(ResponseMode.GET_FIRST, 10000));
        assert !req.isDone();

        req.setResponseFilter(new NonNullFilter());
        req.receiveResponse(null, dests.get(0), false);
        assert !req.isDone();

        req.receiveResponse(true, dests.get(1), false);
        assert req.isDone();
    }

    /**
     * Verifies that a received *and* suspected flag on a Rsp counts only once, to prevent premature termination of
     * a blocking RPC. https://issues.jboss.org/browse/JGRP-1505
     */
    public void testResponsesComplete() {
        GroupRequest<Integer> req=new GroupRequest<>(null, Arrays.asList(a,b,c), RequestOptions.SYNC());
        checkComplete(req, false);

        req.receiveResponse(1, a, false);
        req.receiveResponse(2, b, true);
        checkComplete(req, false);

        req.receiveResponse(3, b, false);
        checkComplete(req, false);

        req.receiveResponse(4, c, false);
        checkComplete(req, true);


        req=new GroupRequest<>(null, Arrays.asList(a,b,c), RequestOptions.SYNC());
        req.receiveResponse(1, a, false);
        checkComplete(req, false);

        req.receiveResponse(2, b, false);
        checkComplete(req, false);

        req.receiveResponse(3, c, false);
        checkComplete(req, true);
    }

    /**
     * Tests https://issues.redhat.com/browse/JGRP-2575:<br/>
     * <pre>
     *   Members: A, B
     *   A uses MessageDispatcher to send a request to B.
     *   B receives the request and starts processing it asynchronously.
     *   For some reason, B decides that A has died. It generates and installs view B3={B}.
     *   B finishes processing the request and tries to send the response. But because B believes A is dead, the response gets discarded.
     *   B notices A is alive. A generates view A4={A,B} and both nodes install it.
     * </pre>
     */
    public void testMissingResponseDueToMergeView() throws Exception {
        try(JChannel ch1=create("A"); JChannel ch2=create("B");
            MessageDispatcher md1=new MessageDispatcher(ch1, r -> "from A");
            MessageDispatcher md2=new MessageDispatcher(ch2, r -> "from B")) {
            Util.waitUntilAllChannelsHaveSameView(10000, 500, ch1,ch2);

            Address a_addr=ch1.getAddress(), b_addr=ch2.getAddress();
            long view_id=ch1.getView().getViewId().getId();

            // the DROP protocol drops the unicast response from B -> A
            DROP drop=new DROP().addDownFilter(m -> Objects.equals(m.getDest(), a_addr));
            ch2.getProtocolStack().insertProtocolAtTop(drop);

            byte[] array=Util.objectToByteBuffer("req from A");
            Buffer buffer=new Buffer(array, 0, array.length);
            CompletableFuture<RspList<Object>> f=md1.castMessageWithFuture(null, buffer, RequestOptions.SYNC());

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

            RspList<Object> rsps=f.get(5, TimeUnit.SECONDS);
            ch2.getProtocolStack().removeProtocol(drop);

            System.out.printf("rsps:\n%s", rsps);
            assert rsps.size() == 2;
            assert rsps.isReceived(a_addr);
            assert !rsps.isReceived(b_addr);
            assert rsps.isSuspected(b_addr);
        }
    }


    public void testResponsesComplete3() {
        Address one=new SiteUUID((UUID)Util.createRandomAddress("lon1"), "lon1", "LON");
        Address two=new SiteUUID((UUID)Util.createRandomAddress("sfo1"), "sfo1", "SFO");
        Address three=new SiteUUID((UUID)Util.createRandomAddress("nyc1"), "nyc1", "NYC");

        GroupRequest<Integer> req=new GroupRequest<>(null, Arrays.asList(one, two, three), RequestOptions.SYNC());
        req.receiveResponse(1, one, false);
        req.siteUnreachable("LON");
        checkComplete(req, false);

        req.siteUnreachable("SFO");
        req.receiveResponse(2, two, false);
        checkComplete(req, false);

        req.siteUnreachable("NYC");
        checkComplete(req, true);
        checkComplete(req, true);
        req.receiveResponse(3, three, false);
        checkComplete(req, true);
    }

    public void testCancel() throws Exception {
        MyCorrelator corr=new MyCorrelator(true, null, 0);
        GroupRequest<Integer> req=new GroupRequest<>(corr, Arrays.asList(a, b, c), RequestOptions.SYNC());
        corr.setGroupRequest(req);
        req.cancel(true);
        RspList<Integer> rsps=req.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 3;
        long num_not_received=rsps.values().stream().filter(rsp -> !rsp.wasReceived()).count();
        assert num_not_received == 3;
        assert req.isCancelled();


        final GroupRequest<Integer> req2=new GroupRequest<>(corr, Arrays.asList(a, b, c), RequestOptions.SYNC());
        corr.setGroupRequest(req2);
        new Thread(() -> {Util.sleep(1000); req2.cancel(true);}).start();
        rsps=req2.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("rsps:\n" + rsps);
        assert rsps.size() == 3;
        num_not_received=rsps.values().stream().filter(rsp -> !rsp.wasReceived()).count();
        assert num_not_received == 3;
        assert req2.isCancelled();
    }


    protected static void checkComplete(GroupRequest req, boolean expect) {
        System.out.println("req = " + req);
        assert req.getResponsesComplete() == expect;
    }


    protected static class NonNullFilter implements RspFilter {
        private volatile boolean validResponse;

        public boolean isAcceptable(Object response, Address sender) {
            if(response != null)
                validResponse=true;
            return response != null;
        }

        public boolean needMoreResponses() {
            return !validResponse;
        }
    }


    /**
     * test group timeout. demonstrates that the timeout mechanism times out too
     * quickly as multiple responses are received by the GroupRequest.
	 * Demonstrates by group request receiving multiple messages in a timeframe
	 * less than the total timeout. the request will fail, as after each
	 * received message, the request alters the total timeout.
	 * 
	 * @throws Exception
	 */
    private void _testMessageTimeout(boolean async) throws Exception {
        
        // need multiple destinations to replicate error
        int destCount = 10;
        
        // total timeout to hear from all members
        final long timeout = destCount * 1000;
        
        // how long each destination should delay
        final long delay = 75L;
        Object[] responses = new Message[destCount];
        
        dests = new ArrayList<>();
        for (int i = 0; i < destCount; i++) {
            Address addr = Util.createRandomAddress();
            dests.add(addr);
            // how long does this simulated destination take to execute? the sum is just less than the total timeout
            responses[i] = new Message(null, (long)i).src(addr);
        }
        
        MyCorrelator corr = new MyCorrelator(async, responses, delay);
        
        // instantiate request with dummy correlator
        GroupRequest<Long> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_ALL, timeout));
        corr.setGroupRequest(req);
        RspList<Long> results=req.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("group request is " + req);
        assert req.isDone();
        Assert.assertEquals(dests.size(), results.size());
    }



    private void _testMessageReception(boolean async) throws Exception {
        Object[] responses={new Message(null,(long)1).src(a),new Message(null,(long)2).src(b)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Object> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
        corr.setGroupRequest(req);
        RspList<Object> results=req.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("group request is " + req);
        assert req.isDone();
        Assert.assertEquals(2, results.size());
    }



    private void _testMessageReceptionWithViewChange(boolean async) throws Exception {
        List<Address> new_dests=new ArrayList<>();
        new_dests.add(a);
        new_dests.add(b);
        new_dests.add(a);
        Object[] responses={new Message(null,(long)1).src(a),
          new View(Util.createRandomAddress(), 322649, new_dests),
          new Message(null,(long)2).src(b)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Long> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
        corr.setGroupRequest(req);
        RspList<Long> results=req.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("group request is " + req);
        assert req.isDone();
        Assert.assertEquals(2, results.size());
    }


    private void _testMessageReceptionWithViewChangeMemberLeft(boolean async) throws Exception {
        List<Address> new_dests=new ArrayList<>();
        new_dests.add(b);
        Object[] responses={new Message(null,(long)1).src(b),
          new View(Util.createRandomAddress(), 322649, new_dests)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Object> req=new GroupRequest<>(corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));

        corr.setGroupRequest(req);
        System.out.println("group request before execution: " + req);
        RspList<Object> results=req.execute(new Buffer(buf, 0, buf.length), true);
        System.out.println("group request after execution: " + req);
        assert req.isDone();
        Assert.assertEquals(2, results.size());
    }


    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack()).name(name).connect("demo");
    }



    protected static class MyCorrelator extends RequestCorrelator {
        protected GroupRequest request;
        protected boolean      async=true;
        protected Object[]     responses=null;
        protected long         delay=0;

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

        public void setGroupRequest(GroupRequest r) {
            request=r;
        }

        @Override
        public void sendRequest(Collection<Address> dest_mbrs, Buffer data, Request req, RequestOptions opts) throws Exception {
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
