
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.SeqnoTable;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.net.UnknownHostException;



@Test(groups=Global.FUNCTIONAL)
public class SeqnoTableTest {
    private static Address MBR=null;

    @BeforeClass
    private static void init() throws UnknownHostException {
        MBR=new IpAddress("127.0.0.1", 5555);
    }


    public static void testInit() {
        SeqnoTable tab=new SeqnoTable(0);
        tab.add(MBR, 0);
        Assert.assertEquals(0, tab.getHighestReceived(MBR));
        Assert.assertEquals(1, tab.getNextToReceive(MBR));

        tab.clear();
        tab=new SeqnoTable(50);
        tab.add(MBR, 50);
        Assert.assertEquals(50, tab.getHighestReceived(MBR));
        Assert.assertEquals(51, tab.getNextToReceive(MBR));
    }


    public static void testAdd() {
        SeqnoTable tab=new SeqnoTable(0);
        tab.add(MBR, 0);
        tab.add(MBR, 1);
        tab.add(MBR, 2);
        Assert.assertEquals(2, tab.getHighestReceived(MBR));
        Assert.assertEquals(3, tab.getNextToReceive(MBR));
    }


    public static void testAddWithGaps() {
        SeqnoTable tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 0);
        assert rc;
        rc=tab.add(MBR, 1);
        assert rc;
        rc=tab.add(MBR, 2);
        assert rc;
        rc=tab.add(MBR, 4);
        assert rc;
        rc=tab.add(MBR, 5);
        assert rc;
        System.out.println("tab: " + tab);
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(3, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 3);
        assert rc;
        System.out.println("tab: " + tab);
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(6, tab.getNextToReceive(MBR));
    }


    public static void testAddWithGaps2() {
        SeqnoTable tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 5);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 4);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 3);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 2);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 1);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(0, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 0);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(6, tab.getNextToReceive(MBR));

    }

    
    public static void testInsertionOfDuplicates() {
        SeqnoTable tab=new SeqnoTable(0);
        boolean rc=tab.add(MBR, 0);
        assert rc;
        rc=tab.add(MBR, 0);
        assert !(rc);

        rc=tab.add(MBR, 1);
        assert rc;
        rc=tab.add(MBR, 2);
        assert rc;
        rc=tab.add(MBR, 4);
        assert rc;
        rc=tab.add(MBR, 5);
        assert rc;
        System.out.println("tab: " + tab);

        rc=tab.add(MBR, 2);
        assert !rc;

        rc=tab.add(MBR, 3);
        assert rc;

        rc=tab.add(MBR, 3);
        assert !rc;
    }

    

}