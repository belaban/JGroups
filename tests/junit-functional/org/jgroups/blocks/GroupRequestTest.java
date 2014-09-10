
package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.RspList;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class GroupRequestTest {
    Address a, b, c;
    List<Address> dests=null;

    @BeforeClass
    void init() throws UnknownHostException {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
    }

    @BeforeMethod
    protected void setUp() throws Exception {
        dests=new ArrayList<Address>(Arrays.asList(a,b));
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
        Object[] responses={new Message(null,a, (long)1),
          new Message(null,b, (long)2),
          new Message(null,c, (long)3)};
        MyCorrelator corr=new MyCorrelator(true, responses, 500);
        dests.add(c);
        GroupRequest<Long> req=new GroupRequest<Long>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_FIRST, 0));
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
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assert rc;
        assert req.isDone();
        RspList<Long> results=req.getResults();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(1, results.numReceived());
    }


    public void testGetAllWithResponseFilter() throws Exception {
        Object[] responses={new Message(null,a, (long)1),
          new Message(null,b, (long)2),
          new Message(null,c, (long)3)};
        MyCorrelator corr=new MyCorrelator(true, responses, 500);
        dests.add(c);
        GroupRequest<Long> req=new GroupRequest<Long>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
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
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assert rc;
        assert req.isDone();
        RspList<Long> results=req.getResults();
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
        GroupRequest<Boolean> req=new GroupRequest<Boolean>(new Message(), null, dests,
                                                            new RequestOptions(ResponseMode.GET_ALL, 10000));
        assert !req.isDone();

        req.setResponseFilter(new NonNullFilter());

        for(Address sender: dests)
            req.receiveResponse(null, sender, false);

        assert req.isDone();
    }
    

    public void testAllNullResponsesWithFilterGetFirst() {
        dests.add(c);
        GroupRequest<Boolean> req=new GroupRequest<Boolean>(new Message(), null, dests,
                                                            new RequestOptions(ResponseMode.GET_FIRST, 10000));
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
        GroupRequest<Integer> req=new GroupRequest<Integer>(null, null, Arrays.asList(a,b,c), RequestOptions.SYNC());
        checkComplete(req, false);

        req.receiveResponse(1, a, false);
        req.receiveResponse(2, b, true);
        checkComplete(req, false);

        req.receiveResponse(3, b, false);
        checkComplete(req, false);

        req.receiveResponse(4, c, false);
        checkComplete(req, true);


        req=new GroupRequest<Integer>(null, null, Arrays.asList(a,b,c), RequestOptions.SYNC());
        req.receiveResponse(1, a, false);
        checkComplete(req, false);

        req.receiveResponse(2, b, false);
        checkComplete(req, false);

        req.suspect(b);
        checkComplete(req, false);

        req.receiveResponse(3, c, false);
        checkComplete(req, true);
    }

    /**
     * Verifies that a received *and* suspected flag on a Rsp counts only once, to prevent premature termination of
     * a blocking RPC. https://issues.jboss.org/browse/JGRP-1505
     */
    public void testResponsesComplete2() {
        GroupRequest<Integer> req=new GroupRequest<Integer>(null, null, Arrays.asList(a,b,c), RequestOptions.SYNC());
        req.suspect(a);
        checkComplete(req, false);
        
        req.receiveResponse(1, a, false);
        checkComplete(req, false);

        req.receiveResponse(2, b, false);
        checkComplete(req, false);

        req.suspect(b);
        checkComplete(req, false);

        req.receiveResponse(3, c, false);
        req.suspect(c);
        checkComplete(req, true);
    }

    public void testResponsesComplete3() {
        Address one=new SiteUUID((UUID)Util.createRandomAddress("lon1"), "lon1", "LON");
        Address two=new SiteUUID((UUID)Util.createRandomAddress("sfo1"), "sfo1", "SFO");
        Address three=new SiteUUID((UUID)Util.createRandomAddress("nyc1"), "nyc1", "NYC");

        GroupRequest<Integer> req=new GroupRequest<Integer>(null, null, Arrays.asList(one, two, three), RequestOptions.SYNC());
        req.suspect(one);
        req.receiveResponse(1, one, false);
        req.siteUnreachable("LON");
        checkComplete(req, false);

        req.siteUnreachable("SFO");
        req.receiveResponse(2, two, false);
        req.suspect(two);
        checkComplete(req, false);

        req.siteUnreachable("NYC");
        checkComplete(req, true);
        req.suspect(three);
        checkComplete(req, true);
        req.receiveResponse(3, three, false);
        checkComplete(req, true);
    }

    protected static void checkComplete(Request req, boolean expect) {
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
        
        dests = new ArrayList<Address>();
        for (int i = 0; i < destCount; i++) {
            Address addr = Util.createRandomAddress();
            dests.add(addr);
            // how long does this simulated destination take to execute? the sum is just less than the total timeout
            responses[i] = new Message(null, addr, (long)i);
        }
        
        MyCorrelator corr = new MyCorrelator(async, responses, delay);
        
        // instantiate request with dummy correlator
        GroupRequest<Long> req=new GroupRequest<Long>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_ALL, timeout));
        corr.setGroupRequest(req);
        boolean rc = req.execute();
        System.out.println("group request is " + req);
        assert rc;
        assert req.isDone();
        RspList<Long> results = req.getResults();
        Assert.assertEquals(dests.size(), results.size());
    }



    private void _testMessageReception(boolean async) throws Exception {
        Object[] responses={new Message(null,a, (long)1),new Message(null,b, (long)2)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Object> req=new GroupRequest<Object>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
        corr.setGroupRequest(req);
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assert rc;
        assert req.isDone();
        RspList<Object> results=req.getResults();
        Assert.assertEquals(2, results.size());
    }



    private void _testMessageReceptionWithViewChange(boolean async) throws Exception {
        List<Address> new_dests=new ArrayList<Address>();
        new_dests.add(a);
        new_dests.add(b);
        new_dests.add(a);
        Object[] responses={new Message(null,a, (long)1),
          new View(Util.createRandomAddress(), 322649, new_dests),
          new Message(null,b, (long)2)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Long> req=new GroupRequest<Long>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));
        corr.setGroupRequest(req);
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assert rc;
        assert req.isDone();
        RspList<Long> results=req.getResults();
        Assert.assertEquals(2, results.size());
    }


    private void _testMessageReceptionWithViewChangeMemberLeft(boolean async) throws Exception {
        List<Address> new_dests=new ArrayList<Address>();
        new_dests.add(b);
        Object[] responses={new Message(null,b, (long)1),
          new View(Util.createRandomAddress(), 322649, new_dests)};
        MyCorrelator corr=new MyCorrelator(async, responses, 0);
        GroupRequest<Object> req=new GroupRequest<Object>(new Message(), corr, dests, new RequestOptions(ResponseMode.GET_ALL, 0));

        corr.setGroupRequest(req);
        System.out.println("group request before execution: " + req);
        boolean rc=req.execute();
        System.out.println("group request after execution: " + req);
        assert rc;
        assert req.isDone();
        RspList<Object> results=req.getResults();
        Assert.assertEquals(2, results.size());
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
        }

        public void setGroupRequest(GroupRequest r) {
            request=r;
        }


        public void sendRequest(long id, List<Address> dest_mbrs, Message msg, RspCollector coll) throws Exception {
            send();
        }

        public void sendRequest(long id, Collection<Address> dest_mbrs, Message msg, RspCollector coll, RequestOptions options) throws Exception {
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
