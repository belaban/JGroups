// $Id: MembershipTest.java,v 1.2 2004/03/30 06:47:31 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.Membership;
import org.jgroups.stack.IpAddress;

import java.util.Vector;




public class MembershipTest extends TestCase {
    Membership m1, m2;
    Vector     v1, v2;
    Address    a1, a2, a3, a4, a5;

    
    
    public MembershipTest(String name) {
	super(name);
    }


    public void setUp() {
	a1=new IpAddress(5555);
	a2=new IpAddress(6666);
	a3=new IpAddress(6666);
	a4=new IpAddress(7777);
	a5=new IpAddress(8888);
	m1=new Membership();
    }

    public void tearDown() {
	
    }

 
    public void testConstructor() {	
	v1=new Vector();
	v1.addElement(a1);
	v1.addElement(a2);
	v1.addElement(a3);
	m2=new Membership(v1);
	assertTrue(m2.size() == 3);
	assertTrue(m2.contains(a1));
	assertTrue(m2.contains(a2));
	assertTrue(m2.contains(a3));
    }


    public void testAdd() {
	m1.add(a1);
	m1.add(a2);
	m1.add(a3);
	assertTrue(m1.size() == 2); // a3 was *not* added because already present (a2)
	assertTrue(m1.contains(a1));
	assertTrue(m1.contains(a2));
	assertTrue(m1.contains(a3));  // a3 is not present, but is equal to a2 !
    }


    public void testAddVector() {
	v1=new Vector();
	v1.addElement(a1);
	v1.addElement(a2);
	v1.addElement(a3);
	m1.add(v1);
	assertTrue(m1.size() == 2);
	assertTrue(m1.contains(a1));
	assertTrue(m1.contains(a2));
    }

    public void testAddVectorDupl() {
	v1=new Vector();
	v1.addElement(a1);
	v1.addElement(a2);
	v1.addElement(a3);
	v1.addElement(a4);
	v1.addElement(a5);

	m1.add(a5);
	m1.add(a1);
	m1.add(v1);
	assertTrue(m1.size() == 4);
	assertTrue(m1.contains(a1));
	assertTrue(m1.contains(a2));
	assertTrue(m1.contains(a4));
	assertTrue(m1.contains(a5));
    }


    public void testRemove() {
	m1.add(a1);
	m1.add(a2);
	m1.add(a3);
	m1.add(a4);
	m1.add(a5);
	m1.remove(a2);
	assertTrue(m1.size() == 3);
    }


    public void testGetMembers() {
	testAdd();
	Vector v=m1.getMembers();
	assertTrue(v.size() == 2);
    }

    
    public void testSet() {
	v1=new Vector();
	v1.addElement(a1);
	v1.addElement(a2);
	m1.add(a1);
	m1.add(a2);
	m1.add(a4);
	m1.add(a5);
	m1.set(v1);
	assertTrue(m1.size() == 2);
	assertTrue(m1.contains(a1));
	assertTrue(m1.contains(a2));
    }


    public void testSet2() {
	m1=new Membership();
	m2=new Membership();
	m1.add(a1);
	m1.add(a2);
	m1.add(a4);
	m2.add(a5);
	m2.set(m1);
	assertTrue(m2.size() == 3);
	assertTrue(m2.contains(a1));
	assertTrue(m2.contains(a2));
	assertTrue(m2.contains(a4));
	assertTrue(!m2.contains(a5));
    } 



    public void testMerge() {
	v1=new Vector(); v2=new Vector();
	m1.add(a1);
	m1.add(a2);
	m1.add(a3);
	m1.add(a4);


	v1.addElement(a5);
	v2.addElement(a2);
	v2.addElement(a3);

	m1.merge(v1, v2);
	assertTrue(m1.size() == 3);
	assertTrue(m1.contains(a1));
	assertTrue(m1.contains(a4));
	assertTrue(m1.contains(a5));
    }



   

    public static Test suite() {
	TestSuite s=new TestSuite(MembershipTest.class);
	return s;
    }

    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
    }
}
