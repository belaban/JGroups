
package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.protocols.PingWaiter;


/**
 * @author Bela Ban
 * @version $Id: PingWaiterTest.java,v 1.1 2005/01/03 11:24:00 belaban Exp $
 */
public class PingWaiterTest extends TestCase {
    PingWaiter waiter;
    
    
    public PingWaiterTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testAllReceived() {
        waiter=new PingWaiter(5000, 3, null);
        
    }

   

    public static Test suite() {
        TestSuite s=new TestSuite(PingWaiterTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
