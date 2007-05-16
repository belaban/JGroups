// $Id: GroupRequestTest.java,v 1.3.2.1 2007/05/16 20:26:11 belaban Exp $$

package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Vector;

public class GroupRequestTest extends TestCase {
    GroupRequest req;
    Address a1, a2;
    Vector dests=new Vector();
    private MyTransport transport;

    public GroupRequestTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
        a1=new IpAddress(1111);
        a2=new IpAddress(2222);
        dests.add(a1);
        dests.add(a2);
    }

    public void testMessageTimeout() throws Exception {
        _testMessageTimeout(true);
    }

    public void testMessageReception() throws Exception {
        _testMessageReception(true);
        _testMessageReception(false);
    }


    public void testMessageReceptionWithSuspect() throws Exception {
        _testMessageReceptionWithSuspect(true);
        _testMessageReceptionWithSuspect(false);
    }


    public void testMessageReceptionWithViewChange() throws Exception {
        _testMessageReceptionWithViewChange(true);
        _testMessageReceptionWithViewChange(false);
    }

    public void testMessageReceptionWithViewChangeMemberLeft() throws Exception {
        _testMessageReceptionWithViewChangeMemberLeft(true);
        _testMessageReceptionWithViewChangeMemberLeft(false);
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
        final long timeout = destCount * 100;

        // how long each destination should delay
        final long delay = 75L;
        Object[] responses = new Message[destCount];

        dests = new Vector();
        for (int i = 0; i < destCount; i++) {
            Address addr = new IpAddress(Integer.parseInt(String.valueOf(i) + i + i + i));
            dests.add(addr);
            // how long does this simulated destination take to execute? the sum is just less than the total timeout
            responses[i] = new Message(null, addr, new Long(i));
        }

        MyDelayedTransport tp = new MyDelayedTransport(async, responses, delay);

        // instantiate request with dummy correlator
        req = new GroupRequest(new Message(), tp, dests,
                               GroupRequest.GET_ALL, timeout, dests.size());
        tp.setGroupRequest(req);
        boolean rc = req.execute();
        System.out.println("group request is " + req);
        assertTrue(rc);
        assertEquals(0, req.getSuspects().size());
        assertTrue(req.isDone());
        RspList results = req.getResults();
        assertEquals(dests.size(), results.size());
    }



    private void _testMessageReception(boolean async) throws Exception {
         Object[] responses=new Message[]{new Message(null, a1, new Long(1)),
                                          new Message(null, a2, new Long(2))};
         transport=new MyTransport(async, responses);
         req=new GroupRequest(new Message(), transport, dests, GroupRequest.GET_ALL, 0, 2);
         transport.setGroupRequest(req);
         boolean rc=req.execute();
         System.out.println("group request is " + req);
         assertTrue(rc);
         assertEquals(0, req.getSuspects().size());
         assertTrue(req.isDone());
         RspList results=req.getResults();
         assertEquals(2, results.size());
     }

    private void _testMessageReceptionWithSuspect(boolean async) throws Exception {
         Object[] responses=new Object[]{new Message(null, a1, new Long(1)),
                                         new SuspectEvent(a2)};
         transport=new MyTransport(async, responses);
         req=new GroupRequest(new Message(), transport, dests, GroupRequest.GET_ALL, 0, 2);
         transport.setGroupRequest(req);
         boolean rc=req.execute();
         System.out.println("group request is " + req);
         assertTrue(rc);
         assertEquals(1, req.getSuspects().size());
         assertTrue(req.isDone());
         RspList results=req.getResults();
         assertEquals(2, results.size());
     }


    private void _testMessageReceptionWithViewChange(boolean async) throws Exception {
        Vector new_dests=new Vector(dests);
        new_dests.add(new IpAddress(3333));
        Object[] responses=new Object[]{new Message(null, a1, new Long(1)),
                                        new View(new IpAddress(9999), 322649, new_dests),
                                        new Message(null, a2, new Long(2))};
        transport=new MyTransport(async, responses);
        req=new GroupRequest(new Message(), transport, dests, GroupRequest.GET_ALL, 0, 2);
        transport.setGroupRequest(req);
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assertTrue(rc);
        assertEquals(0, req.getSuspects().size());
        assertTrue(req.isDone());
        RspList results=req.getResults();
        assertEquals(2, results.size());
    }


    private void _testMessageReceptionWithViewChangeMemberLeft(boolean async) throws Exception {
        Vector new_dests=new Vector(dests);
        new_dests.remove(a1);
        Object[] responses=new Object[]{new Message(null, a2, new Long(1)),
                                        new View(new IpAddress(9999), 322649, new_dests)};
        transport=new MyTransport(async, responses);
        req=new GroupRequest(new Message(), transport, dests, GroupRequest.GET_ALL, 0, 2);
        transport.setGroupRequest(req);
        boolean rc=req.execute();
        System.out.println("group request is " + req);
        assertTrue(rc);
        assertEquals(1, req.getSuspects().size());
        assertTrue(req.isDone());
        RspList results=req.getResults();
        assertEquals(2, results.size());
    }



    public static Test suite() {
        return new TestSuite(GroupRequestTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected static class MyTransport implements Transport {
        GroupRequest request;
        boolean      async=true;
        Object[]     responses=null;

        public MyTransport(boolean async, Object[] responses) {
            this.async=async;
            this.responses=responses;
        }

        public void setGroupRequest(GroupRequest r) {
            request=r;
        }

        public void send(Message msg) throws Exception {
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

        public Object receive(long timeout) throws Exception {
            return null;
        }

        void sendResponses() {
            if(responses != null) {
                Object obj;
                for(int i=0; i < responses.length; i++) {
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
                        request.receiveResponse(retval, sender);
                    }
                    else if(obj instanceof SuspectEvent)
                        request.suspect((Address)((SuspectEvent)obj).getMember());
                    else if(obj instanceof View)
                        request.viewChange((View)obj);
                    else
                        System.err.println("Object needs to be Message, SuspectEvent or View");
                }
            }
        }
    }


    /**
     * transport with set delays between messages
     *
     * @author bgodfrey
     *
     */
    private static final class MyDelayedTransport extends MyTransport {
        long delay;

        public MyDelayedTransport(boolean async, Object[] responses) {
            super(async, responses);
}

        public MyDelayedTransport(boolean async, Object[] responses, long delay) {
            super(async, responses);
            this.delay = delay;
        }


        void sendResponses() {
            if (responses != null) {
                Object obj;
                for (int i = 0; i < responses.length; i++) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }

                    obj = responses[i];
                    if (obj == null) {
                        System.err.println("object was null");
                        continue;
                    }
                    if (obj instanceof Message) {
                        Message msg = (Message) obj;
                        Address sender = msg.getSrc();
                        Object retval = null;
                        try {
                            retval = Util.objectFromByteBuffer(msg.getBuffer());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        request.receiveResponse(retval, sender);
                    } else if (obj instanceof SuspectEvent)
                        request.suspect((Address) ((SuspectEvent) obj).getMember());
                    else if (obj instanceof View)
                        request.viewChange((View) obj);
                    else
                        System.err.println("Object needs to be Message, SuspectEvent or View");
                }
            }
        }
    }

}
