package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.util.PortsManager;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PortManagerTest.java,v 1.3.2.2 2007/11/28 09:17:33 belaban Exp $
 */
public class PortManagerTest extends TestCase {
    PortsManager pm;
    final static int START_PORT=15550;

    protected void setUp() throws Exception {
        super.setUp();
        pm=new PortsManager();
        pm.deleteFile();
    }

    protected void tearDown() throws Exception {
        pm=new PortsManager();
        pm.deleteFile();
        super.tearDown();
    }

    public void testAddition() {
        pm=new PortsManager(30000);
        List<Integer> ports=new LinkedList<Integer>();

        for(int i=0; i < 10; i++) {
            int port=pm.getNextAvailablePort(START_PORT);
            assertTrue(port > 0);
            ports.add(port);
        }

        System.out.println("ports: " + ports);
        assertEquals(10, ports.size());
    }


    public void testNonDuplicateAddition() {
        pm=new PortsManager(30000);

        int port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port=" + port);
        assertEquals(START_PORT, port);

        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        assertNotSame(port, port2);
    }


    public void testExpiration() {
        pm=new PortsManager(800);

        int port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port = " + port);
        Util.sleep(900);
        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        assertEquals(port, port2);

        Util.sleep(900);
        port=pm.getNextAvailablePort(START_PORT);
        port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port=" + port + ", port2=" + port2);
        assertNotSame(port, port2);
    }

    public void testRemove() {
        pm=new PortsManager(10000);
        int port=pm.getNextAvailablePort(START_PORT);
        int old_port=port;
        System.out.println("port = " + port);
        assertEquals(START_PORT, port);
        int port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        assertNotSame(port, port2);
        pm.removePort(port);
        port2=pm.getNextAvailablePort(START_PORT);
        System.out.println("port2 = " + port2);
        assertEquals(port, port2);
        pm.removePort(port);
        pm.removePort(port2);
        port=pm.getNextAvailablePort(START_PORT);
        System.out.println("port = " + port);
        assertEquals(old_port, port);
    }


}
