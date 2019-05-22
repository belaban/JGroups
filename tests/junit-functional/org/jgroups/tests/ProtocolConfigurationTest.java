package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.StackType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests the use of @Property dependency processing and default assignment.
 * @author Richard Achmatowicz
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ProtocolConfigurationTest {
	ProtocolStack stack = null;
	Protocol protocol = null ;
	static final String orderProps="org.jgroups.tests.ProtocolConfigurationTest$ORDERING(a=1;b=2;c=3)";
	static final String refsProps="org.jgroups.tests.ProtocolConfigurationTest$REFS(a=1;b=2;c=3)";
	static final String defaultProps="org.jgroups.tests.ProtocolConfigurationTest$DEFAULTS(b=333)";
	static final String addressProps="org.jgroups.tests.ProtocolConfigurationTest$INETADDRESSES(" + 
									"inetAddressField=127.0.0.1;inet_address_method=192.168.0.100;" + 
									"ipAddressListField=127.0.0.1[8080],127.0.0.1[8081];" + 
									"ip_address_list_method=192.168.0.100[5678],192.168.0.101[2345];port_range=1)" ;
	static final String configurableObjectsProps="org.jgroups.tests.ProtocolConfigurationTest$CONFIGOBJPROTOCOL(" +
	                                "config_object_class=org.jgroups.tests.ProtocolConfigurationTest$ConfigurableObject;" +
	                                "string_property=test)" ;
	          
	List<String> order = new LinkedList<>() ;

	@BeforeMethod
	void setUp() {
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
		List<String> correctOrder = new LinkedList<>() ;
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

		List<ProtocolConfiguration> protocol_configs = new ArrayList<>() ;
		List<Protocol> protocols = new ArrayList<>() ;

		// create the layer described by DEFAULTS
		protocol = Configurator.createProtocol(defaultProps, stack) ;
		// process the defaults
		protocol_configs.add(new ProtocolConfiguration(defaultProps)) ;
		protocols.add(protocol) ;
		Configurator.setDefaultValues(protocol_configs, protocols, StackType.IPv4) ;
		
		// get the value which should have been assigned a default
		int a = ((DEFAULTS)protocol).getA() ;
		System.out.println("value of a = " + a) ;
		
		// get the value which should not have been assigned a default
		int b = ((DEFAULTS)protocol).getB() ;
		System.out.println("value of b = " + b) ;
		
		// assert b == 333 ;
		if (b != 333) {
			throw new RuntimeException("default property value set when it should not have been") ;
		}
		
		// get the value which should not have been assigned a default
		InetAddress c = ((DEFAULTS)protocol).getC() ;
		System.out.println("value of c = " + c) ;
        assert c != null;
	}
	/*
	 * Checks InetAddress and IpAddress processing 
	 */
	public void testAssignmentInetAddresses() throws Exception {
		// create the layer described by INETADDRESSES
		protocol = Configurator.createProtocol(addressProps, stack) ;
				
		// get the value which should have been assigned a default
		InetAddress a = ((INETADDRESSES)protocol).getInetAddressField() ;
		System.out.println("value of inetAddressField = " + a) ;
		
		// get the value which should not have been assigned a default
		InetAddress b = ((INETADDRESSES)protocol).getInetAddressMethod() ;
		System.out.println("value of inetAddressMethod = " + b) ;
		
		// get the value which should have been assigned a default
		List<IpAddress> c = ((INETADDRESSES)protocol).getIpAddressListField() ;
		System.out.println("value of ipAddressListField = " + c) ;
		
		// get the value which should not have been assigned a default
		List<IpAddress> d = ((INETADDRESSES)protocol).getIpAddressListMethod() ;
		System.out.println("value of ipAddressListMethod = " + d) ;

	}
	
	/*
	 * Checks InetAddress and IpAddress processing 
	 */
	public void testConfigurableObject() throws Exception {

		List<ProtocolConfiguration> protocol_configs = new ArrayList<>() ;
		List<Protocol> protocols = new ArrayList<>() ;
		
		// create the layer described by INETADDRESSES
		protocol = Configurator.createProtocol(configurableObjectsProps, stack) ;
		
		// process the defaults (want this eventually)
		protocol_configs.add(new ProtocolConfiguration(configurableObjectsProps)) ;
		protocols.add(protocol) ;
				
		// get the value which should have been assigned a default
		List<Object> configObjs = ((CONFIGOBJPROTOCOL)protocol).getConfigurableObjects() ;
		assert configObjs.size() == 1 ;
		Object configObj = configObjs.get(0) ;
		assert configObj instanceof ConfigurableObject ;  
		assert ((ConfigurableObject)configObj).getStringProp().equals("test") ;
		
	}

	
	public static class ORDERING extends Protocol {
		List<String> list = new LinkedList<>() ;
		
		@Property(name="a", dependsUpon="b") 
		public void setA(int a) {
			list.add("a") ;
		}
		@Property(name="b", dependsUpon="c") 
		public void setB(int b) {
			list.add("b") ;
		}
		@Property(name="c") 
		public void setC(int c) {
			list.add("c") ;
		}
		List<String> getList() {
			return list ;
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

		@Property(name="a", dependsUpon="b") 
		public void setA(int a) {
		}
		@Property(name="b", dependsUpon="d") 
		public void setB(int b) {	
		}
		@Property(name="c") 
		public void setC(int c) {
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
		int a ;
		int b ;
		InetAddress c ;
		
		@Property(name="a")
		public void setA(int a) {
			this.a = a ;
		}
		@Property(name="b") 
		public void setB(int b) {
			this.b = b ;
		}
		@Property(name="c", defaultValueIPv4="192.168.1.10") 
		public void setC(InetAddress ia) {
			this.c = ia ;
		}
		public int getA() {
			return a ;
		}
		public int getB() {
			return b ;
		}
		public InetAddress getC() {
			return c ;
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
	public static class INETADDRESSES extends Protocol {
		InetAddress inetAddressMethod ;
		
		@Property(name="inetAddressField")
		InetAddress inetAddressField ;
		
		public InetAddress getInetAddressField() {
			return inetAddressField ;
		}
		@Property(name="inetAddressMethod") 
		public void setInetAddressMethod(InetAddress ia) {
			this.inetAddressMethod = ia ;
		}
		public InetAddress getInetAddressMethod() {
			return inetAddressMethod ;
		}

		@Property(description="fred")
		int port_range = 0 ;
		
		// List<IpAddress> - uses InitialHosts converter
		List<IpAddress> ipAddressListMethod ;
		
		@Property(name="ipAddressListField", converter=PropertyConverters.InitialHosts.class)
		List<IpAddress> ipAddressListField ;
		
		public List<IpAddress> getIpAddressListField() {
			return ipAddressListField ;
		}
		@Property(name="ipAddressListMethod", converter=PropertyConverters.InitialHosts.class, dependsUpon="port_range") 
		public void setIpAddressListMethod(List<IpAddress> ia) {
			this.ipAddressListMethod = ia ;
		}
		public List<IpAddress> getIpAddressListMethod() {
			return ipAddressListMethod ;
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
	public static class CONFIGOBJPROTOCOL extends Protocol {

	    private Object configObjInstance=null;
		
	    @Property(name="config_object_class")
	    public void setConfigurableObjectClass(String class_name) throws Exception {
            configObjInstance=Class.forName(class_name).getDeclaredConstructor().newInstance();
	    }
	    
	    public List<Object> getConfigurableObjects() {
	        List<Object> retval=new LinkedList<>();
	        if(configObjInstance != null)
	            retval.add(configObjInstance);
	        return retval;
	    }
		

		// do nothing
		public Object up(Event evt) {
			return up_prot.up(evt);
		}
	}

	public static class ConfigurableObject {
		@Property(name="string_property")
		String stringProp = null ;
		public String getStringProp() {
			return stringProp ;
		}
		public void setStringProp(String s) {
			this.stringProp = s ;
		}
	}
}        
