// $Id: DigestTest.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.tests;


import junit.framework.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.protocols.pbcast.Digest;



public class DigestTest extends TestCase {
    Digest     d;
    IpAddress  a1, a2, a3;    
    
    public DigestTest(String name) {
	super(name);
    }


    public void setUp() {
	d=new Digest(3);
	a1=new IpAddress(5555);
	a2=new IpAddress(6666);
	a3=new IpAddress(7777);
	d.add(a1, 4, 500, 501);
	d.add(a2, 25, 26, 26);
	d.add(a3, 20, 25, 33);
    }

    public void tearDown() {
	
    }

    
    public void testConstructor() {
	assertTrue(d.size() == 3);
	d.reset(2);
	assertTrue(d.size() == 2);
	d.reset(4);
	assertTrue(d.size() == 4);
    }
    


    public void testConstructor2() {
	Digest dd=new Digest(3);
	assertTrue(dd.lowSeqnoAt(0) == 0);
	assertTrue(dd.highSeqnoAt(0) == 0);
	assertTrue(dd.highSeqnoSeenAt(0) == -1);
    }


    
    public void testGetIndex() {
	assertTrue(d.getIndex(a1) == 0);
	assertTrue(d.getIndex(a3) == 2);
    }
    


    public void testContains() {
	assertTrue(d.contains(a2));
    }


    public void testSenderAt() {
	assertTrue(d.senderAt(2).equals(a3));
    }

    
    public void testResetAt() {
	d.resetAt(0);
	assertTrue(d.lowSeqnoAt(0)      == 0);
	assertTrue(d.highSeqnoAt(0)     == 0);
	assertTrue(d.highSeqnoSeenAt(0) == -1);
    }

    
    public void testLowSeqnoAt() {
	assertTrue(d.lowSeqnoAt(0) == 4);
	assertTrue(d.lowSeqnoAt(1) == 25);
	assertTrue(d.lowSeqnoAt(2) == 20);
    }

    
    public void testHighSeqnoAt() {
	assertTrue(d.highSeqnoAt(0) == 500);
	assertTrue(d.highSeqnoAt(1) == 26);
	assertTrue(d.highSeqnoAt(2) == 25);
    }
    
    public void testHighSeqnoSeenAt() {
	assertTrue(d.highSeqnoSeenAt(0) == 501);
	assertTrue(d.highSeqnoSeenAt(1) == 26);
	assertTrue(d.highSeqnoSeenAt(2) == 33);
    }

    public void testCopy() {
	d=d.copy();
	testLowSeqnoAt();
	testHighSeqnoAt();
	testHighSeqnoSeenAt();
	testGetIndex();
	testContains();
	testSenderAt();
	testResetAt();
    }


    public void testNonConflictingMerge() {
	Digest    cons_d=new Digest(5);
	IpAddress ip1=new IpAddress(1111), ip2=new IpAddress(2222);

	cons_d.add(ip1, 1, 10, 10);
	cons_d.add(ip2, 2, 20, 20);
	// System.out.println("\ncons_d before: " + cons_d);
	cons_d.merge(d);
	//System.out.println("\ncons_d after: " + cons_d);
	assertTrue(cons_d.getIndex(ip1) == 0);
	assertTrue(cons_d.getIndex(ip2) == 1);
	assertTrue(cons_d.getIndex(a1)  == 2);
	assertTrue(cons_d.getIndex(a2)  == 3);
	assertTrue(cons_d.getIndex(a3)  == 4);
	assertTrue(cons_d.lowSeqnoAt(0) == 1);
	assertTrue(cons_d.lowSeqnoAt(1) == 2);
	assertTrue(cons_d.lowSeqnoAt(2) == 4);
	assertTrue(cons_d.lowSeqnoAt(3) == 25);
	assertTrue(cons_d.lowSeqnoAt(4) == 20);
	assertTrue(cons_d.highSeqnoAt(0) == 10);
	assertTrue(cons_d.highSeqnoAt(1) == 20);
	assertTrue(cons_d.highSeqnoAt(2) == 500);
	assertTrue(cons_d.highSeqnoAt(3) == 26);
	assertTrue(cons_d.highSeqnoAt(4) == 25);
	assertTrue(cons_d.highSeqnoSeenAt(0) == 10);
	assertTrue(cons_d.highSeqnoSeenAt(1) == 20);
	assertTrue(cons_d.highSeqnoSeenAt(2) == 501);
	assertTrue(cons_d.highSeqnoSeenAt(3) == 26);
	assertTrue(cons_d.highSeqnoSeenAt(4) == 33);
    }


    public void testConflictingMerge() {
	Digest new_d=new Digest(2);
	new_d.add(a1, 5, 450, 501);
	new_d.add(a3, 18, 28, 35);
	//System.out.println("\nd before: " + d);
	//System.out.println("new_: " + new_d);
	d.merge(new_d);
	//System.out.println("d after: " + d);
	
	assertTrue(d.lowSeqnoAt(0)      ==   4);  // low_seqno should *not* have changed
	assertTrue(d.highSeqnoAt(0)     == 500);  // high_seqno should *not* have changed
	assertTrue(d.highSeqnoSeenAt(0) == 501);  // high_seqno_seen should *not* have changed

	assertTrue(d.lowSeqnoAt(1)      ==  25);  // low_seqno should *not* have changed
	assertTrue(d.highSeqnoAt(1)     ==  26);  // high_seqno should *not* have changed
	assertTrue(d.highSeqnoSeenAt(1) ==  26);  // high_seqno_seen should *not* have changed

	assertTrue(d.lowSeqnoAt(2)      ==  18);  // low_seqno should *not* have changed
	assertTrue(d.highSeqnoAt(2)     ==  28);  // high_seqno should *not* have changed
	assertTrue(d.highSeqnoSeenAt(2) ==  35);  // high_seqno_seen should *not* have changed
    }

   

    public static Test suite() {
	TestSuite s=new TestSuite(DigestTest.class);
	return s;
    }

    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}
