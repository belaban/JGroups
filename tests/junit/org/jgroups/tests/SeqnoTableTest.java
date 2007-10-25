
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.SeqnoTable;

import java.net.UnknownHostException;


public class SeqnoTableTest extends TestCase {
    SeqnoTable tab;
    private static Address MBR;

    static {
        try {
            MBR=new IpAddress("127.0.0.1", 5555);
        }
        catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public SeqnoTableTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        tab.clear();
    }

    public void testInit() {
        tab=new SeqnoTable(0);
        tab.add(MBR, 0);
        assertEquals(0, tab.getHighestReceived(MBR));
        assertEquals(1, tab.getNextToReceive(MBR));

        tab.clear();
        tab=new SeqnoTable(50);
        tab.add(MBR, 50);
        assertEquals(50, tab.getHighestReceived(MBR));
        assertEquals(51, tab.getNextToReceive(MBR));
    }

    public void testAdd() {
        tab=new SeqnoTable(0);
        tab.add(MBR, 0);
        tab.add(MBR, 1);
        tab.add(MBR, 2);
        assertEquals(2, tab.getHighestReceived(MBR));
        assertEquals(3, tab.getNextToReceive(MBR));
    }

    public void testAddWithGaps() {
        tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 0);
        assertTrue(rc);
        rc=tab.add(MBR, 1);
        assertTrue(rc);
        rc=tab.add(MBR, 2);
        assertTrue(rc);
        rc=tab.add(MBR, 4);
        assertTrue(rc);
        rc=tab.add(MBR, 5);
        assertTrue(rc);
        System.out.println("tab: " + tab);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(3, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 3);
        assertTrue(rc);
        System.out.println("tab: " + tab);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(6, tab.getNextToReceive(MBR));
    }

    public void testAddWithGaps2() {
        tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 5);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 4);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 3);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 2);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 1);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 0);
        System.out.println("tab: " + tab);
        assertTrue(rc);
        assertEquals(5, tab.getHighestReceived(MBR));
        assertEquals(6, tab.getNextToReceive(MBR));

    }

    public void testInsertionOfDuplicates() {
        tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 0);
        assertTrue(rc);
        rc=tab.add(MBR, 0);
        assertFalse(rc);

        rc=tab.add(MBR, 1);
        assertTrue(rc);
        rc=tab.add(MBR, 2);
        assertTrue(rc);
        rc=tab.add(MBR, 4);
        assertTrue(rc);
        rc=tab.add(MBR, 5);
        assertTrue(rc);
        System.out.println("tab: " + tab);

        rc=tab.add(MBR, 2);
        assertFalse(rc);

        rc=tab.add(MBR, 3);
        assertTrue(rc);

        rc=tab.add(MBR, 3);
        assertFalse(rc);
    }

    

    public static Test suite() {
        return new TestSuite(SeqnoTableTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}