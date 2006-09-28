package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.JChannel;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 * Tests the FLUSH protocol, requires flush-udp.xml in ./conf to be present and configured to use FLUSH
 * @author Bela Ban
 * @version $Id: FlushTest.java,v 1.1 2006/09/28 10:04:09 belaban Exp $
 */
public class FlushTest extends TestCase {
    Channel c1, c2;
    static final String CONFIG="flush-udp.xml";


    public FlushTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();

        if(c2 != null) {
            c2.close();
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            c1=null;
        }
    }


    public void testSingleChannel() throws ChannelException {
        c1=createChannel();
        MyReceiver receiver=new MyReceiver();
        c1.setReceiver(receiver);
        c1.connect("bla");
        List events=receiver.getEvents();
        System.out.println("events: " + events);
        assertEquals(0, events.size());

        c1.close();
        events=receiver.getEvents();
        System.out.println("events: " + events);
        assertEquals(0, events.size());
    }






    private Channel createChannel() throws ChannelException {
        Channel ret=new JChannel(CONFIG);
        ret.setOpt(Channel.BLOCK, Boolean.TRUE);
        return ret;
    }


    public static Test suite() {
        return new TestSuite(FlushTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(FlushTest.suite());
    }

    private static class MyReceiver extends ExtendedReceiverAdapter {
        List events=new LinkedList();

        public void clear() {
            events.clear();
        }

        public List getEvents() {return Collections.unmodifiableList(events);}

        public void block() {

        }

        public void unblock() {

        }
    }
}
