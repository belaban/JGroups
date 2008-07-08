package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.PortsManager;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PortManagerTest.java,v 1.5 2008/07/08 21:05:30 vlada Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class PortManagerTest {
    private final static int START_PORT=15550;


    public static void testAddition() {
        PortsManager pm=new PortsManager(30000,"testAddition.txt");
        pm.deleteFile();
        List<Integer> ports=new LinkedList<Integer>();

        for(int i=0; i < 10; i++) {
            int port=pm.getNextAvailablePort(START_PORT);
            assert port > 0;
            ports.add(port);
        }

        System.out.println("ports: " + ports);
        Assert.assertEquals(10, ports.size());
    }


    public static void testNonDuplicateAddition() {
        PortsManager pm=new PortsManager(30000,"testNonDuplicateAddition.txt");
        pm.deleteFile();

        int port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port=" + port);
        Assert.assertEquals(START_PORT, port);

        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        Assert.assertNotSame(port, port2);
    }


    public static void testExpiration() {
        PortsManager pm=new PortsManager(800,"testExpiration.txt");
        pm.deleteFile();

        int port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port = " + port);
        Util.sleep(900);
        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        Assert.assertEquals(port, port2);

        Util.sleep(900);
        port=pm.getNextAvailablePort(START_PORT);
        port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port=" + port + ", port2=" + port2);
        Assert.assertNotSame(port, port2);
    }


    public static void testRemove() {
        PortsManager pm=new PortsManager(10000,"testRemove.txt");
        pm.deleteFile();
        int port=pm.getNextAvailablePort(START_PORT);
        int old_port=port;
        System.out.println("port = " + port);
        Assert.assertEquals(START_PORT, port);
        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        Assert.assertNotSame(port, port2);
        pm.removePort(port);
        port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        Assert.assertEquals(port, port2);
        pm.removePort(port);
        pm.removePort(port2);
        port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port = " + port);
        Assert.assertEquals(old_port, port);
    }


}
