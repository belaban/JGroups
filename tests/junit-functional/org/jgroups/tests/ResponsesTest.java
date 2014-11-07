package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.PingData;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class ResponsesTest {
    protected static final int NUM=10;
    protected static final String[] names=new String[NUM];
    protected static final Address[] addrs=new Address[NUM];
    protected static final PhysicalAddress[] phys_addrs=new PhysicalAddress[NUM];

    static {
        for(int i=0; i < 10; i++) {
            names[i]=String.valueOf((char)('A' + i));
            addrs[i]=Util.createRandomAddress(names[i]);
            phys_addrs[i]=new IpAddress(5000 + i);
        }
    }


    public void testAddResponses() throws Exception {
        Responses rsps=new Responses(10, true);
        System.out.println("rsps = " + rsps);
        assert !rsps.isDone();
        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true, names[i], phys_addrs[i]), false);
        System.out.println("rsps = " + rsps);
        assert !rsps.isDone();
        assert !rsps.waitFor(500);

        for(int i=0; i < 5; i++)
            assert rsps.containsResponseFrom(addrs[i]);
        assert !rsps.containsResponseFrom(addrs[5]);

        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true, names[i], phys_addrs[i]), false);
        System.out.println("rsps = " + rsps);
        assert !rsps.isDone() && rsps.size() == 5;

        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true, names[i], phys_addrs[i]), true);
        System.out.println("rsps = " + rsps);
        assert !rsps.isDone() && rsps.size() == 5;

        for(int i=5; i < 10; i++)
            rsps.addResponse(new PingData(addrs[i], true, names[i], phys_addrs[i]), false);
        System.out.println("rsps = " + rsps);
        assert rsps.isDone() && rsps.size() == 10;
        assert rsps.waitFor(60000);
    }

    public void testContainsResponse() {
        Responses rsps=new Responses(10, true);
        assert !rsps.isDone();
        for(int i=0; i<5;i++)
            rsps.addResponse(new PingData(addrs[i], true,names[i], phys_addrs[i]), false);
        System.out.println("rsps = "+rsps);
        assert rsps.containsResponseFrom(addrs[3]);
        PingData rsp=rsps.findResponseFrom(addrs[3]);
        assert rsp != null && rsp.getAddress().equals(addrs[3]);
    }

    public void testResize() throws Exception {
        Responses rsps=new Responses(5, true, 3);
        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true,names[i],phys_addrs[i]), false);
        assert rsps.size() == 5;
    }

    public void testSizeOfOne() {
        Responses rsps=new Responses(1, true, 1);
        rsps.addResponse(new PingData(addrs[0],true,names[0],phys_addrs[0]),false);
        assert rsps.isDone();
    }

    public void testBreakOnCoordRsp() {
        Responses rsps=new Responses(true);
        rsps.addResponse(new PingData(addrs[0],true,names[0],phys_addrs[0]), false);
        assert !rsps.isDone();

        rsps.addResponse(new PingData(addrs[1],true,names[1],phys_addrs[1]).coord(true), false);
        System.out.println("rsps = " + rsps);
        assert rsps.isDone();
    }

    public void testClear() {
        Responses rsps=new Responses(10, true);
        System.out.println("rsps = " + rsps);
        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true,names[i],phys_addrs[i]), false);
        System.out.println("rsps = " + rsps);
        assert rsps.size() == 5;
        assert !rsps.isDone();
        rsps.clear();
        System.out.println("rsps = " + rsps);
        assert rsps.isEmpty();
        assert rsps.isDone();
    }

    public void testWaitFor() throws Exception {
        final Responses rsps=new Responses(5, true);
        boolean done=rsps.waitFor(500);
        assert !done;
        long start=System.currentTimeMillis();
        new Thread() {
            public void run() {
                Util.sleep(500);
                for(int i=5; i < 10; i++)
                    rsps.addResponse(new PingData(addrs[i],true,names[i],phys_addrs[i]), false);
            }
        }.start();

        done=rsps.waitFor(20000);
        long time=System.currentTimeMillis() - start;
        System.out.printf("rsps (in %d ms) = %s\n", time, rsps);
        assert done;
        assert rsps.size() == 5;
    }

    public void testWaitFor2() throws Exception {
        final Responses rsps=new Responses(5, true);
        boolean done=rsps.waitFor(500);
        assert !done;
        long start=System.currentTimeMillis();
        new Thread() {
            public void run() {
                Util.sleep(500);
                for(int i=0; i < 2; i++)
                    rsps.addResponse(new PingData(addrs[i],true,names[i],phys_addrs[i]), false);
                rsps.addResponse(new PingData(addrs[3], true, names[3], phys_addrs[3]).coord(true), false);
            }
        }.start();

        done=rsps.waitFor(20000);
        long time=System.currentTimeMillis() - start;
        System.out.printf("rsps (in %d ms) = %s\n", time, rsps);
        assert done;
        assert rsps.size() == 3;
    }

    public void testWaitForOnDone() {
        final Responses rsps=new Responses(5, true).done();
        boolean done=rsps.waitFor(500);
        assert done;
    }

    /** Tests JGRP-1899 */
    public void testWaitFor3() {
        Responses rsps=new Responses(0, true, 5);
        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i], true, names[i], phys_addrs[i]), false);

        boolean complete=rsps.waitFor(5);
        assert !complete;

        rsps.addResponse(new PingData(addrs[5], true, names[5], phys_addrs[5]).coord(true), false);
        complete=rsps.waitFor(500);
        assert complete;
    }

    public void testIterator() throws Exception {
        Responses rsps=new Responses(10, true);
        for(int i=0; i < 5; i++)
            rsps.addResponse(new PingData(addrs[i],true,names[i],phys_addrs[i]), false);
        int count=0;
        for(PingData data: rsps) {
            if(data != null)
                count++;
            if(count == 2)
                rsps.addResponse(new PingData(addrs[5],true,names[5],phys_addrs[5]), false);
        }
        assert count == 5;
    }

    public void testIterator2() {
        Responses rsps=new Responses(10, true);
        int cnt=0;
        for(PingData data: rsps)
            if(data != null)
                cnt++;
        assert cnt == 0;
    }

}
