package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global ;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Configurator ;
import org.jgroups.stack.Configurator.InetAddressInfo;
import org.jgroups.stack.Configurator.ProtocolConfiguration;
import org.jgroups.annotations.Property; 
import org.jgroups.util.Util ;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.net.InetAddress ;

/**
 * Tests checks made on InetAddress and related addresses in Configurator.
 * @author Richard Achmatowicz
 * @version $Id: InetAddressChecksTest.java,v 1.2 2009/10/13 21:52:12 rachmatowicz Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class InetAddressChecksTest {
	ProtocolStack stack = null;
	ProtocolConfiguration protocol_config = null ;
	Protocol protocol = null ;
	static final String ipCheckNoConsistentProps="org.jgroups.tests.InetAddressChecksTest$IPCHECK(" + 
								"inetAddress1=127.0.0.1;inetAddress2=::1;inetAddress3=192.168.0.100;i=3)" ;
	static final String ipCheckConsistentProps="org.jgroups.tests.InetAddressChecksTest$IPCHECK(" + 
								"inetAddress1=127.0.0.1;inetAddress2=127.0.0.1;inetAddress3=192.168.0.100;i=3)" ;
	          
	List<String> order = new LinkedList<String>() ;

	@BeforeMethod
	void setUp() throws Exception {
		stack=new ProtocolStack();
	}

	/*
	 * Checks IP version mechanism for inconsistent version processing
	 */
	@Test(expectedExceptions=RuntimeException.class)
	public void testIPVersionCheckingNoConsistentVersion() throws Exception {

		Vector<ProtocolConfiguration> protocol_configs = new Vector<ProtocolConfiguration>() ;
		Vector<Protocol> protocols = new Vector<Protocol>() ;
		
		// create the layer described by IPCHECK
		protocol = Configurator.createProtocol(ipCheckNoConsistentProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(ipCheckNoConsistentProps)) ;
		protocols.add(protocol) ;
		
        Map<String, Map<String,InetAddressInfo>> inetAddressMap = null ;
        boolean assumeIPv4 ;
		try {
	        inetAddressMap = Configurator.createInetAddressMap(protocol_configs, protocols) ;
	        assumeIPv4 = Configurator.getIPVersion(inetAddressMap) ;
		}
		catch(RuntimeException e) {
			System.out.println("Expected exception received: " + e.getMessage()) ;
			throw e ;
		}
		
		// get the value which should have been assigned a default
		InetAddress a = ((IPCHECK)protocol).getInetAddress1() ;
		System.out.println("value of inetAddress1 = " + a) ;
		
		InetAddress b = ((IPCHECK)protocol).getInetAddress2() ;
		System.out.println("value of inetAddress2 = " + b) ;
		
		InetAddress c = ((IPCHECK)protocol).getInetAddress3() ;
		System.out.println("value of inetAddress3 = " + c) ;
		
	}

	/*
	 * Checks IP version mechanism for consistent version processing
	 */
	public void testIPVersionCheckingConsistentVersion() throws Exception {

		Vector<ProtocolConfiguration> protocol_configs = new Vector<ProtocolConfiguration>() ;
		Vector<Protocol> protocols = new Vector<Protocol>() ;
		
		// create the layer described by IPCHECK
		protocol = Configurator.createProtocol(ipCheckConsistentProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(ipCheckConsistentProps)) ;
		protocols.add(protocol) ;
		
		Map<String, Map<String,InetAddressInfo>> inetAddressMap = null ;
		boolean assumeIPv4 ;

		inetAddressMap = Configurator.createInetAddressMap(protocol_configs, protocols) ;
		assumeIPv4 = Configurator.getIPVersion(inetAddressMap) ;

		// get the value which should have been assigned a default
		InetAddress a = ((IPCHECK)protocol).getInetAddress1() ;
		System.out.println("value of inetAddress1 = " + a) ;
		
		InetAddress b = ((IPCHECK)protocol).getInetAddress2() ;
		System.out.println("value of inetAddress2 = " + b) ;
		
		InetAddress c = ((IPCHECK)protocol).getInetAddress3() ;
		System.out.println("value of inetAddress3 = " + c) ;
		
	}
	/*
	 * Checks which IP stacks are available on the platform
	 */
	public void testWhichIPStacksAvailable() throws Exception {

		boolean isIPv4 = Util.isIPv4StackAvailable() ;
		boolean isIPv6 = Util.isIPv6StackAvailable() ;
		
		System.out.println("isIPv4 = " + isIPv4);
		System.out.println("isIPv6 = " + isIPv6);
	}

	
	public static class IPCHECK extends Protocol {
		String name = "IPCHECK" ;
		
		@Property(name="inetAddress1")
		InetAddress inetAddress1 ;	
		public InetAddress getInetAddress1() {
			return inetAddress1 ;
		}
		@Property(name="inetAddress2")
		InetAddress inetAddress2 ;	
		public InetAddress getInetAddress2() {
			return inetAddress2 ;
		}
		@Property(name="inetAddress3")
		InetAddress inetAddress3 ;	
		public InetAddress getInetAddress3() {
			return inetAddress3 ;
		}
		
		@Property(description="wilma") 
		int i = 0 ;
		
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
