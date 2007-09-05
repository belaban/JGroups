
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.protocols.pbcast.NAKACK;



/**
 * Tests LossRate
 * @author Bela Ban
 * @version $Id: LossRateTest.java,v 1.2 2007/09/05 08:13:08 belaban Exp $ 
 */
public class LossRateTest extends TestCase {
    NAKACK.LossRate lr=new NAKACK.LossRate();

    public LossRateTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testEmpty() {
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(0.0, loss_rate);
    }

    public void testNoMissingMessages() {
        for(int i=1; i <= 10; i++)
            lr.addReceived(i);
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(0.0, loss_rate);
    }

    public void testAllMissingMessages() {
        lr.addMissing(1, 10);
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(1.0, loss_rate);
    }

    public void testSomeMissingMessages() {
        lr.addReceived(1L,2L,9L,10L);
        lr.addMissing(3, 8);
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(0.6, loss_rate);

        lr.addReceived(3);
        System.out.println("lr: " + lr);
        loss_rate=lr.computeLossRate();
        assertEquals(0.5, loss_rate);

        lr.addReceived(7L,8L);
        System.out.println("lr: " + lr);
        loss_rate=lr.computeLossRate();
        assertEquals(0.3, loss_rate);

        lr.addReceived(5L,6L);
        System.out.println("lr: " + lr);
        loss_rate=lr.computeLossRate();
        assertEquals(0.1, loss_rate);

        lr.addReceived(4);
        System.out.println("lr: " + lr);
        loss_rate=lr.computeLossRate();
        assertEquals(0.0, loss_rate);
    }


    public void testMissingThenReceivedMessages() {
        lr.addMissing(1,2);
        System.out.println("lr: " + lr);
        lr.addReceived(1L,2L);
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(0.0, loss_rate);
    }
    

    public void testConflictingMissingAndReceivedMessages() {
        lr.addReceived(1L,2L);
        System.out.println("lr: " + lr);
        lr.addMissing(2,2);
        System.out.println("lr: " + lr);
        double loss_rate=lr.computeLossRate();
        assertEquals(0.0, loss_rate);
    }

    public static void main(String[] args) {
        String[] testCaseName={LossRateTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}