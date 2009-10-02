package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global ;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Configurator ;
import org.jgroups.stack.Configurator.ProtocolConfiguration;
import org.jgroups.annotations.Property; 
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.IllegalArgumentException;
import java.util.List;
import java.util.LinkedList;
import java.util.Vector;

/**
 * Tests the use of @Property dependency processing and default assignment.
 * @author Richard Achmatowicz
 * @version $Id: ProtocolConfigurationTest.java,v 1.1 2009/10/02 18:54:30 rachmatowicz Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ProtocolConfigurationTest {
	ProtocolStack stack = null;
	ProtocolConfiguration protocol_config = null ;
	Protocol protocol = null ;
	static final String orderProps="org.jgroups.tests.ProtocolConfigurationTest$ORDERING(a=1;b=2;c=3)";
	static final String refsProps="org.jgroups.tests.ProtocolConfigurationTest$REFS(a=1;b=2;c=3)";
	static final String defaultProps="org.jgroups.tests.ProtocolConfigurationTest$DEFAULTS(b=333)";
	List<String> order = new LinkedList<String>() ;

	@BeforeMethod
	void setUp() throws Exception {
		stack=new ProtocolStack();
	}

	/*
	 * Checks that missing dependencies are flagged
	 */
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testResolutionOfDependencies() throws Exception {
		
		// create the layer described by REFS
		try {
			protocol = Configurator.createProtocol(refsProps, stack) ;
		}
		catch(IllegalArgumentException e) {
			System.out.println("exception thrown (expected): " + e.getMessage());
			// rethrow to make sure testNG does not fail the test
			throw e ;
		}
	}
	
	/*
	 * Checks that dependency ordering works
	 */
	public void testDependencyOrdering() throws Exception {
		// create a List describing correct Property ordering
		List<String> correctOrder = new LinkedList<String>() ;
		correctOrder.add("c") ;
		correctOrder.add("b") ;
		correctOrder.add("a") ;

		// create the layer described by ORDERING
		protocol = Configurator.createProtocol(orderProps, stack) ;
		
		// check that the list elements are in the right order
		List<String> actualOrder = ((ORDERING)protocol).getList() ;
		
		assert actualOrder.equals(correctOrder) ;
	}

	/*
	 * Checks assignment of defaults
	 */
	public void testDefaultAssignment() throws Exception {

		Vector<ProtocolConfiguration> protocol_configs = new Vector<ProtocolConfiguration>() ;
		Vector<Protocol> protocols = new Vector<Protocol>() ;
		
		// create the layer described by DEFAULTS
		protocol = Configurator.createProtocol(defaultProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(defaultProps)) ;
		protocols.add(protocol) ;
		Configurator.processDefaultValues(protocol_configs, protocols) ;
		
		// get the value which should have been assigned a default
		int a = ((DEFAULTS)protocol).getA() ;
		System.out.println("value of a = " + a) ;
		
		// get the value which should not have been assigned a default
		int b = ((DEFAULTS)protocol).getB() ;
		System.out.println("value of b = " + b) ;
		
		// assert a == 111 ;
		if (a != 111) {
			throw new RuntimeException("default property value not set") ;
		}
		// assert b == 333 ;
		if (b != 333) {
			throw new RuntimeException("default property value set when it should not have been") ;
		}

	}

	
	public static class ORDERING extends Protocol {
		String name = "ORDERING" ;
		List<String> list = new LinkedList<String>() ;
		
		@Property(name="a", dependsUpon="b") 
		public void setA(int a) {
			// add letter to a list
			list.add("a") ;
		}
		@Property(name="b", dependsUpon="c") 
		public void setB(int b) {
			// add B to a list
			list.add("b") ;			
		}
		@Property(name="c") 
		public void setC(int c) {
			// add C to a list
			list.add("c") ;
		}
		List<String> getList() {
			return list ;
		}
		public String getName() {
			return name ;
		}
		// do nothing
		public Object down(Event evt) {
			return down_prot.down(evt);
		}
		// do nothing
		public Object up(Event evt) {
			return up_prot.up(evt);
		}
	}
	public static class REFS extends Protocol {
		String name = "REFS" ;
		
		@Property(name="a", dependsUpon="b") 
		public void setA(int a) {
		}
		@Property(name="b", dependsUpon="d") 
		public void setB(int b) {	
		}
		@Property(name="c") 
		public void setC(int c) {
		}
		public String getName() {
			return name ;
		}
		// do nothing
		public Object down(Event evt) {
			return down_prot.down(evt);
		}
		// do nothing
		public Object up(Event evt) {
			return up_prot.up(evt);
		}
	}
	public static class DEFAULTS extends Protocol {
		String name = "DEFAULTS" ;
		int a ;
		int b ;
		
		@Property(name="a", defaultValue="111") 
		public void setA(int a) {
			this.a = a ;
		}
		@Property(name="b", defaultValue="222") 
		public void setB(int b) {
			this.b = b ;
		}
		public int getA() {
			return a ;
		}
		public int getB() {
			return b ;
		}
		public String getName() {
			return name ;
		}
		// do nothing
		public Object down(Event evt) {
			return down_prot.down(evt);
		}
		// do nothing
		public Object up(Event evt) {
			return up_prot.up(evt);
		}
	}
	
}        
