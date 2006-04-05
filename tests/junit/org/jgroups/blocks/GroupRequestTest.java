// $Id: GroupRequestTest.java,v 1.2 2006/04/05 05:38:36 belaban Exp $$

package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.RspList;

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

    private static final class MyTransport implements Transport {
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
                    if(obj instanceof Message)
                        request.receiveResponse((Message)obj);
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
}
