
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.SeqnoTable;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;



@Test(groups=Global.FUNCTIONAL)
public class SeqnoTableTest {
    private static Address MBR=null;

    @BeforeClass
    private static void init() throws UnknownHostException {
        MBR=Util.createRandomAddress();
    }


    public static void testInit() {
        SeqnoTable tab=new SeqnoTable();
        tab.add(MBR, 0);
        Assert.assertEquals(0, tab.getHighestReceived(MBR));
        Assert.assertEquals(1, tab.getNextToReceive(MBR));

        tab.clear();
        tab=new SeqnoTable();
        tab.add(MBR, 50);
        Assert.assertEquals(50, tab.getHighestReceived(MBR));
        Assert.assertEquals(51, tab.getNextToReceive(MBR));
    }


    public static void testAdd() {
        SeqnoTable tab=new SeqnoTable();
        tab.add(MBR, 0);
        tab.add(MBR, 1);
        tab.add(MBR, 2);
        Assert.assertEquals(2, tab.getHighestReceived(MBR));
        Assert.assertEquals(3, tab.getNextToReceive(MBR));
    }


    public static void testAddWithGaps() {
        SeqnoTable tab=new SeqnoTable();
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
        SeqnoTable tab=new SeqnoTable();
        boolean rc=tab.add(MBR, 5);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(5, tab.getHighestReceived(MBR));
        Assert.assertEquals(6, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 10);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(10, tab.getHighestReceived(MBR));
        Assert.assertEquals(6, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 6);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(10, tab.getHighestReceived(MBR));
        Assert.assertEquals(7, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 7);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(10, tab.getHighestReceived(MBR));
        Assert.assertEquals(8, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 8);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(10, tab.getHighestReceived(MBR));
        Assert.assertEquals(9, tab.getNextToReceive(MBR));

        rc=tab.add(MBR, 9);
        System.out.println("tab: " + tab);
        assert rc;
        Assert.assertEquals(10, tab.getHighestReceived(MBR));
        Assert.assertEquals(11, tab.getNextToReceive(MBR));

    }


    public static void testInsertionOfDuplicates() {
        SeqnoTable tab=new SeqnoTable();
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

        rc=tab.add(MBR, 4);
        assert !rc;

        rc=tab.add(MBR, 3);
        assert rc;

        rc=tab.add(MBR, 3);
        assert !rc;
    }



}