// $Id: AckMcastSenderWindowTest.java,v 1.5 2008/04/08 12:17:07 belaban Exp $
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Global;
import org.jgroups.stack.AckMcastSenderWindow;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.StaticInterval;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.*;


/**
 * Test <code>AckMcastSenderWindow</code>
 * <p/>
 * <code>testAck()</code>:<br>
 * 1. Create two messages {1,2} each with 3 distinct destinations.<br>
 * 2. Start a thread that acknowledges messages between sleeping
 * intervals.<br>
 * 3. When the callback retransmission function is called, check that the
 * request is for a destination that is still associated with the given
 * message sequence number.<br>
 * <p/>
 * Since <code>AckMcastSenderWindow</code> does not export its state, keep
 * track of seqnos and address lists in a hashtable.
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class AckMcastSenderWindowTest {

    /**
     * A list of destination addresses
     */
    private static Address[] _RECVS={
            new IpAddress(5000),
            new IpAddress(5001),
            new IpAddress(5002)
    };


    /**
     * The retransmit command
     */
    private AckMcastSenderWindow.RetransmitCommand _cmd;
    /**
     * The mcast retransmit window
     */
    private AckMcastSenderWindow _win;
    /**
     * 2-level table
     * seqNo -> list of destinations
     */
    private final Hashtable _tbl=new Hashtable();


    @BeforeMethod
    void setUp() throws Exception {
        _cmd=new Cmd();
        _win=new AckMcastSenderWindow(_cmd);

    }

    @AfterMethod
    void tearDown() throws Exception {
        _win.stop();
    }



    /**
     * Add 2 messages to 3 destinations
     * <p/>
     * Start acknowledging messages while checking the validity of
     * retransmissions
     */
    public void test1() {
        Vector dests=new Vector();
        Message msg=new Message();
        Acker acker=new Acker();
        long seqno;

        dests.addAll(Arrays.asList(_RECVS));

        // seqno/1
        seqno=1;
        for(int i=0; i < _RECVS.length; ++i) _put(seqno, _RECVS[i]);
        _win.add(seqno, msg, dests);

        // seqno/2
        seqno=2;
        for(int i=0; i < _RECVS.length; ++i) _put(seqno, _RECVS[i]);
        _win.add(seqno, msg, dests);

        // start
        acker.start();
        try {
            acker.join();
        }
        catch(InterruptedException ex) {
            ex.printStackTrace();
        }

        _win.stop();
    }


    public static void testRemove() throws UnknownHostException {
        AckMcastSenderWindow mywin=new AckMcastSenderWindow(new MyCommand(), new StaticInterval(1000, 2000, 3000));
        Address sender1=new IpAddress("127.0.0.1", 10000);
        Address sender2=new IpAddress("127.0.0.1", 10001);
        Address sender3=new IpAddress("127.0.0.1", 10002);
        Vector senders=new Vector();
        Message msg=new Message();
        long seqno=322649;

        senders.addElement(sender1);
        senders.addElement(sender2);
        senders.addElement(sender3);

        mywin.add(seqno, msg, (Vector)senders.clone()); // clone() for the fun of it...

        mywin.ack(seqno, sender1);
        mywin.ack(seqno, sender2);

        System.out.println("entry is " + mywin.printDetails(seqno));
        Assert.assertEquals(3, mywin.getNumberOfResponsesExpected(seqno));
        Assert.assertEquals(2, mywin.getNumberOfResponsesReceived(seqno));
        mywin.waitUntilAllAcksReceived(4000);
        mywin.suspect(sender3);
        Assert.assertEquals(0, mywin.size());
    }





    private class Cmd implements AckMcastSenderWindow.RetransmitCommand {

        public void retransmit(long seqno, Message msg, Address addr) {
            _retransmit(seqno, msg, addr);
        }
    }

    private class Acker extends Thread {
        public void run() {
            _ackerRun();
        }
    }



    /**
     * Associate the given addess with this sequence number. This is to
     * reflect the state of the <code>AckMcastSenderWindow</code> as its state
     * is not exported
     * @param seqno the sequence number
     * @param addr  the address to associate with the seqno
     */
    private void _put(long seqno, Address addr) {
        List list;

        synchronized(_tbl) {
            if((list=(List)_tbl.get(new Long(seqno))) == null) {
                list=new ArrayList();
                _tbl.put(new Long(seqno), list);
            }
            if(!list.contains(addr)) list.add(addr);
            else {
                if(list.isEmpty()) _tbl.remove(new Long(seqno));
            }
        } // synchronized(_tbl)
    }

    /**
     * Remove the given address from the list of addresses for this seqno
     * @param seqno the sequence number associated with a list of addresses
     * @param addr  the address to remove from the list of addresses mapped
     *              to this seqno
     */
    private void _remove(long seqno, Address addr) {
        List list;

        synchronized(_tbl) {
            if((list=(List)_tbl.get(new Long(seqno))) == null) return;
            list.remove(addr);
            if(list.isEmpty()) _tbl.remove(new Long(seqno));
        } // synchronized(_tbl)
    }

    /**
     * @return true if <code>addr</code> is associated with <code>seqno</code>
     */
    private boolean _contains(long seqno, Address addr) {
        List list;

        synchronized(_tbl) {
            if((list=(List)_tbl.get(new Long(seqno))) == null) return (false);
            return (list.contains(addr));
        } // synchronized(_tbl)
    }


    /**
     * Thread acknowledging messages
     */
    private void _ackerRun() {
        // Ack {2, _RECVS[2]}
        _win.ack(2, _RECVS[2]);
        _remove(2, _RECVS[2]);
        try {
            Thread.sleep(1000);
        }
        catch(InterruptedException ex) {
            ex.printStackTrace();
        }

        // Ack {1, _RECVS[1]}
        _win.ack(1, _RECVS[1]);
        _remove(1, _RECVS[1]);
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException ex) {
            ex.printStackTrace();
        }

        // Ack {1, _RECVS[0]}
        // Ack {2, _RECVS[0]}
        // Ack {2, _RECVS[1]}
        _win.ack(1, _RECVS[0]);
        _remove(1, _RECVS[0]);
        _win.ack(2, _RECVS[0]);
        _remove(2, _RECVS[0]);
        _win.ack(2, _RECVS[1]);
        _remove(2, _RECVS[1]);
        try {
            Thread.sleep(500);
        }
        catch(InterruptedException ex) {
            ex.printStackTrace();
        }

        // Ack {1, _RECVS[2]}
        _win.ack(1, _RECVS[2]);
        _remove(1, _RECVS[2]);
    }


    /**
     * Check if retransmission is expected
     */
    private void _retransmit(long seqno, Message msg, Address addr) {
        if(!_contains(seqno, addr))
            assert false : "Acknowledging a non-existent msg, great!";
        else
            System.out.println("retransmitting " + seqno + ", msg=" + msg);
    }





    static class MyCommand implements AckMcastSenderWindow.RetransmitCommand {

        public void retransmit(long seqno, Message msg, Address dest) {
            System.out.println("-- retransmitting " + seqno);
        }
    }



}
