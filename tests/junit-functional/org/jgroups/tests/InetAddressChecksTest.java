package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Configurator.InetAddressInfo;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.*;

/**
 * Tests checks made on InetAddress and related addresses in Configurator.
 * @author Richard Achmatowicz
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class InetAddressChecksTest {
	ProtocolStack stack = null;
	Protocol protocol = null ;
	static final String ipCheckNoConsistentProps="org.jgroups.tests.InetAddressChecksTest$IPCHECK(" + 
								"inetAddress1=127.0.0.1;inetAddress2=::1;inetAddress3=192.168.0.100;i=3)" ;
	static final String ipCheckConsistentProps="org.jgroups.tests.InetAddressChecksTest$IPCHECK(" + 
								"inetAddress1=127.0.0.1;inetAddress2=127.0.0.1;inetAddress3=192.168.0.100;i=3)" ;
	          
	List<String> order = new LinkedList<>() ;

	@BeforeMethod
	void setUp() {
		stack=new ProtocolStack();
	}

	/*
	 * Checks IP version mechanism for inconsistent version processing
	 */
	@Test(expectedExceptions=RuntimeException.class)
	public void testIPVersionCheckingNoConsistentVersion() throws Exception {
		List<ProtocolConfiguration> protocol_configs = new ArrayList<>() ;
		List<Protocol> protocols = new ArrayList<>() ;
		
		// create the layer described by IPCHECK
		protocol = Configurator.createProtocol(ipCheckNoConsistentProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(ipCheckNoConsistentProps)) ;
		protocols.add(protocol) ;
		
        Map<String, Map<String,InetAddressInfo>> inetAddressMap = null ;
		try {
	        inetAddressMap = Configurator.createInetAddressMap(protocol_configs, protocols) ;
            Collection<InetAddress> addrs=Configurator.getAddresses(inetAddressMap);
	        determineIpVersionFromAddresses(addrs) ;
		}
		catch(RuntimeException e) {
			System.out.println("Expected exception received: " + e.getMessage()) ;
			throw e ;
		}
	}

	/*
	 * Checks IP version mechanism for consistent version processing
	 */
	public void testIPVersionCheckingConsistentVersion() throws Exception {

		List<ProtocolConfiguration> protocol_configs = new ArrayList<>() ;
		List<Protocol> protocols = new ArrayList<>() ;
		
		// create the layer described by IPCHECK
		protocol = Configurator.createProtocol(ipCheckConsistentProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(ipCheckConsistentProps)) ;
		protocols.add(protocol) ;
		
		Map<String, Map<String,InetAddressInfo>> inetAddressMap = null ;

		inetAddressMap = Configurator.createInetAddressMap(protocol_configs, protocols) ;
        Collection<InetAddress> addrs=Configurator.getAddresses(inetAddressMap);
		determineIpVersionFromAddresses(addrs) ;

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
	public static void testWhichIPStacksAvailable() throws Exception {

		boolean isIPv4 = Util.isStackAvailable(true);
		boolean isIPv6 = Util.isStackAvailable(false);
		
		System.out.println("isIPv4 = " + isIPv4);
		System.out.println("isIPv6 = " + isIPv6);
	}

	
	public static class IPCHECK extends Protocol {

		@Property(name="inetAddress1")
		InetAddress inetAddress1;

		public InetAddress getInetAddress1() {
			return inetAddress1;
		}

		@Property(name="inetAddress2")
		InetAddress inetAddress2;

		public InetAddress getInetAddress2() {
			return inetAddress2;
		}

		@Property(name="inetAddress3")
		InetAddress inetAddress3;

		public InetAddress getInetAddress3() {
			return inetAddress3;
		}

		@Property(description="wilma")
		int i=0;
	}

	/**
	 * This method takes a set of InetAddresses, represented by an inetAddressmap, and:
	 * - if the resulting set is non-empty, goes through to see if all InetAddress-related
	 *   user settings have a consistent IP version: v4 or v6, and throws an exception if not
	 * - if the resulting set is empty, sets the default IP version based on available stacks
	 *   and if a dual stack, stack preferences
	 * - sets the IP version to be used in the JGroups session
	 * @return StackType.IPv4 for IPv4, StackType.IPv6 for IPv6, StackType.DUAL for dual stacks
	 */
	protected static void determineIpVersionFromAddresses(Collection<InetAddress> addrs) throws Exception {
		Class<? extends InetAddress> clazz=addrs.iterator().next().getClass();
		if(!addrs.stream().allMatch(addr -> addr.getClass() == clazz))
			throw new RuntimeException("all addresses have to be either IPv4 or IPv6: " + addrs);
	}
}        
