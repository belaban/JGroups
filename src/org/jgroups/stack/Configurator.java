package org.jgroups.stack;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.conf.PropertyHelper;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack.ProtocolStackFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.AccessibleObject ;
import java.lang.reflect.Type ;
import java.lang.reflect.ParameterizedType ;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.InetAddress ;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress ;


/**
 * The task if this class is to setup and configure the protocol stack. A string describing
 * the desired setup, which is both the layering and the configuration of each layer, is
 * given to the configurator which creates and configures the protocol stack and returns
 * a reference to the top layer (Protocol).<p>
 * Future functionality will include the capability to dynamically modify the layering
 * of the protocol stack and the properties of each layer.
 * @author Bela Ban
 * @author Richard Achmatowicz
 * @version $Id: Configurator.java,v 1.68 2009/10/07 20:38:23 rachmatowicz Exp $
 */
public class Configurator implements ProtocolStackFactory {

    protected static final Log log=LogFactory.getLog(Configurator.class);
    private final ProtocolStack stack;
     
    public Configurator() {      
        stack = null;
    }

    public Configurator(ProtocolStack protocolStack) {
         stack=protocolStack;
    }

    public Protocol setupProtocolStack() throws Exception{
         return setupProtocolStack(stack.getSetupString(), stack);
    }
     
    public Protocol setupProtocolStack(ProtocolStack copySource)throws Exception{
        Vector<Protocol> protocols=copySource.copyProtocols(stack);
        Collections.reverse(protocols);
        return connectProtocols(protocols);                  
    }
     
     


    /**
     * The configuration string has a number of entries, separated by a ':' (colon).
     * Each entry consists of the name of the protocol, followed by an optional configuration
     * of that protocol. The configuration is enclosed in parentheses, and contains entries
     * which are name/value pairs connected with an assignment sign (=) and separated by
     * a semicolon.
     * <pre>UDP(in_port=5555;out_port=4445):FRAG(frag_size=1024)</pre><p>
     * The <em>first</em> entry defines the <em>bottommost</em> layer, the string is parsed
     * left to right and the protocol stack constructed bottom up. Example: the string
     * "UDP(in_port=5555):FRAG(frag_size=32000):DEBUG" results is the following stack:<pre>
     *
     *   -----------------------
     *  | DEBUG                 |
     *  |-----------------------|
     *  | FRAG frag_size=32000  |
     *  |-----------------------|
     *  | UDP in_port=32000     |
     *   -----------------------
     * </pre>
     */
    private Protocol setupProtocolStack(String configuration, ProtocolStack st) throws Exception {       
        Vector<ProtocolConfiguration> protocol_configs=parseConfigurations(configuration);
        Vector<Protocol> protocols=createProtocols(protocol_configs, st);
        if(protocols == null)
            return null;
        // basic protocol sanity check
        sanityCheck(protocols);
        
        // check InetAddress related features of stack
        Map<String, Map<String,InetAddressInfo>> inetAddressMap = null ;
        boolean assumeIPv4 ;
        inetAddressMap = createInetAddressMap(protocol_configs, protocols) ;
        assumeIPv4 = getIPVersion(inetAddressMap) ;
        if (!assumeIPv4) {
        	checkIPv6Scopes(inetAddressMap) ;
        }
        // process default values
        processDefaultValues(protocol_configs, protocols, assumeIPv4) ;
        
        return connectProtocols(protocols);        
    }

    /**
     * Creates a new protocol given the protocol specification. Initializes the properties and starts the
     * up and down handler threads.
     * @param prot_spec The specification of the protocol. Same convention as for specifying a protocol stack.
     *                  An exception will be thrown if the class cannot be created. Example:
     *                  <pre>"VERIFY_SUSPECT(timeout=1500)"</pre> Note that no colons (:) have to be
     *                  specified
     * @param stack The protocol stack
     * @return Protocol The newly created protocol
     * @exception Exception Will be thrown when the new protocol cannot be created
     */
    public static Protocol createProtocol(String prot_spec, ProtocolStack stack) throws Exception {
        ProtocolConfiguration config;
        Protocol prot;

        if(prot_spec == null) throw new Exception("Configurator.createProtocol(): prot_spec is null");

        // parse the configuration for this protocol
        config=new ProtocolConfiguration(prot_spec);

        // create an instance of the protocol class and configure it
        prot=config.createLayer(stack);
        prot.init();
        return prot;
    }


   

    /* ------------------------------- Private Methods ------------------------------------- */


    /**
     * Creates a protocol stack by iterating through the protocol list and connecting
     * adjacent layers. The list starts with the topmost layer and has the bottommost
     * layer at the tail.
     * @param protocol_list List of Protocol elements (from top to bottom)
     * @return Protocol stack
     */
    private Protocol connectProtocols(Vector<Protocol> protocol_list) {
        Protocol current_layer=null, next_layer=null;

        for(int i=0; i < protocol_list.size(); i++) {
            current_layer=protocol_list.elementAt(i);
            if(i + 1 >= protocol_list.size())
                break;
            next_layer=protocol_list.elementAt(i + 1);
            next_layer.setDownProtocol(current_layer);
            current_layer.setUpProtocol(next_layer);

             if(current_layer instanceof TP) {
                TP transport = (TP)current_layer;                
                if(transport.isSingleton()) {                   
                    ConcurrentMap<String, Protocol> up_prots=transport.getUpProtocols();
                    String key;
                    synchronized(up_prots) {
                        while(true) {
                            key=Global.DUMMY + System.currentTimeMillis();
                            if(up_prots.containsKey(key))
                                continue;
                            up_prots.put(key, next_layer);
                            break;
                        }
                    }
                    current_layer.setUpProtocol(null);
                }
            }
        }
        return current_layer;
    }


    /**
     * Get a string of the form "P1(config_str1):P2:P3(config_str3)" and return
     * ProtocolConfigurations for it. That means, parse "P1(config_str1)", "P2" and
     * "P3(config_str3)"
     * @param config_str Configuration string
     * @return Vector of strings
     */
    private Vector<String> parseProtocols(String config_str) throws IOException {
        Vector<String> retval=new Vector<String>();
        PushbackReader reader=new PushbackReader(new StringReader(config_str));
        int ch;
        StringBuilder sb;
        boolean running=true;

        while(running) {
            String protocol_name=readWord(reader);
            sb=new StringBuilder();
            sb.append(protocol_name);

            ch=read(reader);
            if(ch == -1) {
                retval.add(sb.toString());
                break;
            }

            if(ch == ':') {  // no attrs defined
                retval.add(sb.toString());
                continue;
            }

            if(ch == '(') { // more attrs defined
                reader.unread(ch);
                String attrs=readUntil(reader, ')');
                sb.append(attrs);
                retval.add(sb.toString());
            }
            else {
                retval.add(sb.toString());
            }

            while(true) {
                ch=read(reader);
                if(ch == ':') {
                    break;
                }
                if(ch == -1) {
                    running=false;
                    break;
                }
            }
        }
        reader.close();

        return retval;
    }


    private static int read(Reader reader) throws IOException {
        int ch=-1;
        while((ch=reader.read()) != -1) {
            if(!Character.isWhitespace(ch))
                return ch;
        }
        return ch;
    }

    /**
     * Return a number of ProtocolConfigurations in a vector
     * @param configuration protocol-stack configuration string
     * @return Vector of ProtocolConfigurations
     */
    public Vector<ProtocolConfiguration> parseConfigurations(String configuration) throws Exception {
        Vector<ProtocolConfiguration> retval=new Vector<ProtocolConfiguration>();
        Vector<String> protocol_string=parseProtocols(configuration);              

        if(protocol_string == null)
            return null;
        
        for(String component_string:protocol_string) {                       
            retval.addElement(new ProtocolConfiguration(component_string));
        }
        return retval;
    }



    private static String readUntil(Reader reader, char c) throws IOException {
        StringBuilder sb=new StringBuilder();
        int ch;
        while((ch=read(reader)) != -1) {
            sb.append((char)ch);
            if(ch == c)
                break;
        }
        return sb.toString();
    }

    private static String readWord(PushbackReader reader) throws IOException {
        StringBuilder sb=new StringBuilder();
        int ch;

        while((ch=read(reader)) != -1) {
            if(Character.isLetterOrDigit(ch) || ch == '_' || ch == '.' || ch == '$') {
                sb.append((char)ch);
            }
            else {
                reader.unread(ch);
                break;
            }
        }

        return sb.toString();
    }


    /**
     * Takes vector of ProtocolConfigurations, iterates through it, creates Protocol for
     * each ProtocolConfiguration and returns all Protocols in a vector.
     * @param protocol_configs Vector of ProtocolConfigurations
     * @param stack The protocol stack
     * @return Vector of Protocols
     */
    private Vector<Protocol> createProtocols(Vector<ProtocolConfiguration> protocol_configs, final ProtocolStack stack) throws Exception {
        Vector<Protocol> retval=new Vector<Protocol>();
        ProtocolConfiguration protocol_config;
        Protocol layer;
        String singleton_name;

        for(int i=0; i < protocol_configs.size(); i++) {
            protocol_config=protocol_configs.elementAt(i);
            singleton_name=protocol_config.getProperties().getProperty(Global.SINGLETON_NAME);
            if(singleton_name != null && singleton_name.trim().length() > 0) {
               Map<String,Tuple<TP, ProtocolStack.RefCounter>> singleton_transports=ProtocolStack.getSingletonTransports();
                synchronized(singleton_transports) {
                    if(i > 0) { // crude way to check whether protocol is a transport
                        throw new IllegalArgumentException("Property 'singleton_name' can only be used in a transport" +
                                " protocol (was used in " + protocol_config.getProtocolName() + ")");
                    }
                    Tuple<TP, ProtocolStack.RefCounter> val=singleton_transports.get(singleton_name);
                    layer=val != null? val.getVal1() : null;
                    if(layer != null) {
                        retval.add(layer);
                    }
                    else {
                        layer=protocol_config.createLayer(stack);
                        if(layer == null)
                            return null;
                        singleton_transports.put(singleton_name, new Tuple<TP, ProtocolStack.RefCounter>((TP)layer,new ProtocolStack.RefCounter((short)0,(short)0)));
                        retval.addElement(layer);
                    }
                }
                continue;
            }
            layer=protocol_config.createLayer(stack);
            if(layer == null)
                return null;
            retval.addElement(layer);
        }
        return retval;
    }


    /**
     Throws an exception if sanity check fails. Possible sanity check is uniqueness of all protocol names
     */
    public static void sanityCheck(Vector<Protocol> protocols) throws Exception {
        Vector<String> names=new Vector<String>();
        Protocol prot;
        String name;       
        Vector<ProtocolReq> req_list=new Vector<ProtocolReq>();        

        // Checks for unique names
        for(int i=0; i < protocols.size(); i++) {
            prot=protocols.elementAt(i);
            name=prot.getName();
            for(int j=0; j < names.size(); j++) {
                if(name.equals(names.elementAt(j))) {
                    throw new Exception("Configurator.sanityCheck(): protocol name " + name +
                            " has been used more than once; protocol names have to be unique !");
                }
            }
            names.addElement(name);
        }


        // Checks whether all requirements of all layers are met
        for(Protocol p:protocols){           
            req_list.add(new ProtocolReq(p));
        }    
        
        for(ProtocolReq pr:req_list){
            for(Integer evt_type:pr.up_reqs) {                
                if(!providesDownServices(req_list, evt_type)) {
                    throw new Exception("Configurator.sanityCheck(): event " +
                            Event.type2String(evt_type) + " is required by " +
                            pr.name + ", but not provided by any of the layers above");
                }
            } 
            
            for(Integer evt_type:pr.down_reqs) {                
                if(!providesUpServices(req_list, evt_type)) {
                    throw new Exception("Configurator.sanityCheck(): event " +
                            Event.type2String(evt_type) + " is required by " +
                            pr.name + ", but not provided by any of the layers above");
                }
            }                     
        }            
    }
    
    
    /**
     * This method takes a set of InetAddresses, represented by an inetAddressmap, and:
     * - if the resulting set is non-empty, goes through to see if all InetAddress-related 
     * user settings have a consistent IP version: v4 or v6, and throws an exception if not
     * - if the resulting set is empty, sets the default IP version based on available stacks 
     * and if a dual stack, stack preferences 
     * - sets the IP version to be used in the JGroups session
     */
    public static boolean getIPVersion(Map<String, Map<String,InetAddressInfo>> inetAddressMap) throws Exception {
    	
    	// for each InetAddressInfo with non-null value, add to sets
    	// sets which hold user those IP addresses specified by user (non-null)
    	Set<InetAddress> userSpecified = new HashSet<InetAddress>() ;
    	Set<InetAddress> userSpecifiedIPv4 = new HashSet<InetAddress>() ;
    	Set<InetAddress> userSpecifiedIPv6 = new HashSet<InetAddress>() ;

		for (Map.Entry<String,Map<String, InetAddressInfo>> inetAddressMapEntry : inetAddressMap.entrySet()) {
			String protocol = inetAddressMapEntry.getKey() ;
			Map<String, InetAddressInfo> protocolInetAddressMap = inetAddressMapEntry.getValue() ; 
			for (Map.Entry<String, InetAddressInfo> protocolInetAddressMapEntry : protocolInetAddressMap.entrySet()) {
				String propertyName = protocolInetAddressMapEntry.getKey() ;
				InetAddressInfo inetAddressInfo = protocolInetAddressMapEntry.getValue() ; 
				// add InetAddressInfo to sets based on IP version
				List<InetAddress> addresses = inetAddressInfo.getInetAddresses();
				for (InetAddress address: addresses) {
					if (address == null) 
						throw new RuntimeException("This address should not be null! - something is wrong") ;
					userSpecified.add(address) ;
					if (address instanceof Inet4Address)
						userSpecifiedIPv4.add(address) ;
					else 
						userSpecifiedIPv6.add(address) ;
				}
			}
		}    	
		
		if (log.isDebugEnabled()) {
			log.debug("userSpecified set contents:") ;
			for (Iterator<InetAddress> iter = userSpecified.iterator(); iter.hasNext();) {
				InetAddress address = iter.next() ;
				log.debug(address.toString()) ;
			}
			log.debug("userSpecifiedIPv4 set contents:") ;
			for (Iterator<InetAddress> iter = userSpecifiedIPv4.iterator(); iter.hasNext();) {
				InetAddress address = iter.next() ;
				log.debug(address.toString()) ;
			}
			log.debug("userSpecifiedIPv6 set contents:") ;
			for (Iterator<InetAddress> iter = userSpecifiedIPv6.iterator(); iter.hasNext();) {
				InetAddress address = iter.next() ;
				log.debug(address.toString()) ;
			}
		}
		
    	// now use sets to compute IP version
		boolean isIPv4StackAvailable = Util.isIPv4StackAvailable() ;
		boolean isIPv6StackAvailable = Util.isIPv6StackAvailable() ;
		boolean assumeIPv4 = true ;
		
    	if (log.isDebugEnabled()) {
    		log.debug("isIPv4StackAvailable = " + isIPv4StackAvailable) ;
    		log.debug("isIPv6StackAvailable = " + isIPv6StackAvailable) ;
    	}
		
		
		// the user supplied 1 or more IP address inputs. Check if we have a consistent set
    	// which matches a stack on this host
    	if (userSpecified.size() > 0) {
    		// check for a consistent IP version for IP addresses specified
    		if (!((userSpecifiedIPv4.size() > 0 && userSpecifiedIPv6.size() == 0) || 
    				(userSpecifiedIPv4.size() == 0 && userSpecifiedIPv6.size() > 0))) {
    			throw new RuntimeException("No consistent IP version available (IPv4 or IPv6) for user-specified IP addresses") ;
    		}
    		// set the consistent version - we don't reach here unless we have one
    		assumeIPv4 = (userSpecifiedIPv4.size() > 0 && userSpecifiedIPv6.size() == 0) ;
    		
    		if (assumeIPv4 && !isIPv4StackAvailable) {
    			throw new RuntimeException("A consistent IP version (IPv4) is available for " + 
    					"user-specified addresses but there is no stack to support it") ;
    		}
    		if (!assumeIPv4 && !isIPv6StackAvailable) {
    			throw new RuntimeException("A consistent IP version (IPv6) is available for " + 
				" user-specified addresses but there is no stack to support it") ;
    		}	
    	}
    	else {
    		// the user supplied no default IP addresses, so just get the stack preference.
    		
    		// if only IPv4 stack available, set assumeIPv4 = true
    		if (isIPv4StackAvailable && !isIPv6StackAvailable) {
    			assumeIPv4 = true ;
    		}
    		else if (isIPv6StackAvailable && !isIPv4StackAvailable) {
    			assumeIPv4 = false ;
    		}
    		else if (isIPv4StackAvailable && isIPv6StackAvailable) {
    			// get the System property which records user preference for a stack on a dual stack machine
    			// Bela wanted to check for null and set to IPv4
    			boolean preferIPv4Stack = Boolean.getBoolean("java.net.preferIPv4Stack") ;
    			assumeIPv4 = preferIPv4Stack ;
    		}
    	}
    	
    	if (log.isDebugEnabled()) {
    		log.debug("assumeIPv4 = " + assumeIPv4) ;
    	}
    	
    	return assumeIPv4 ;
    }    
    
    public static void checkIPv6Scopes(Map<String, Map<String,InetAddressInfo>> inetAddressMap) throws Exception {
    	
    	// for each IPv6 address specified, check that if a link-local address is used, it has a scope
		for (Map.Entry<String,Map<String, InetAddressInfo>> inetAddressMapEntry : inetAddressMap.entrySet()) {
			String protocol = inetAddressMapEntry.getKey() ;
			Map<String, InetAddressInfo> protocolInetAddressMap = inetAddressMapEntry.getValue() ; 
			for (Map.Entry<String, InetAddressInfo> protocolInetAddressMapEntry : protocolInetAddressMap.entrySet()) {
				String propertyName = protocolInetAddressMapEntry.getKey() ;
				InetAddressInfo inetAddressInfo = protocolInetAddressMapEntry.getValue() ; 
				List<InetAddress> addresses = inetAddressInfo.getInetAddresses();
				for (InetAddress address: addresses) {
					if (address == null) 
						throw new RuntimeException("This address should not be null! - something is wrong") ;
					
					// check if each link-local address has a scope
					if (address instanceof Inet6Address && address.isLinkLocalAddress()) {
						// check scope is present
						String propertyValue = inetAddressInfo.getStringValue() ;
						if (propertyValue == null)
							throw new RuntimeException("The string value for this address should not be null! - something is wrong") ;

						//check for scope and scope value here!
						if (log.isDebugEnabled()) 
							log.debug("Checking scope for Inet6 address " + address.getHostName() + 
									" with user specified value " + propertyValue) ;
						// TODO - implement the check
					}
				}
			}
		}    	
    }	
    
    /*
     * A method which does the following:
     * - discovers all Fields or Methods within the protocol stack which set
     * InetAddress, IpAddress, InetSocketAddress (and Lists of such) for which the user *has*
     * specified a default value.
	 * - stores the resulting set of Fields and Methods in a map of the form:
     *  Protocol -> Property -> InetAddressInfo 
     * where InetAddressInfo instances encapsulate the InetAddress related information 
     * of the Fields and Methods.
     */
    public static Map<String, Map<String,InetAddressInfo>> createInetAddressMap(Vector<ProtocolConfiguration> protocol_configs, 
    		Vector<Protocol> protocols) throws Exception {
    	// Map protocol -> Map<String, InetAddressInfo>, where the latter is protocol specific
    	Map<String, Map<String,InetAddressInfo>> inetAddressMap = new HashMap<String, Map<String, InetAddressInfo>>() ;

    	// collect InetAddressInfo
    	for (int i = 0; i < protocol_configs.size(); i++) {    		        	
    		ProtocolConfiguration protocol_config = protocol_configs.get(i) ;
    		Protocol protocol = protocols.get(i) ;
    		String protocolName = protocol.getName();

    		// maps property names to InetAddressInfo objects
    		Map<String, InetAddressInfo> protocolInetAddressMap = new HashMap<String,InetAddressInfo>() ;
    		inetAddressMap.put(protocolName, protocolInetAddressMap) ;

    		Properties properties = null ;
    		// regenerate the Properties which were destroyed during basic property processing
    		properties = protocol_config.getOriginalProperties();

			if (log.isDebugEnabled())
				log.debug("Processing InetAddressInfo for protocol: " + protocolName);

    		// check which InetAddress-related properties are ***non-null ***, and 
    		// create an InetAddressInfo structure for them
    		Method[] methods=protocol.getClass().getMethods();
    		for(int j = 0; j < methods.length; j++) {
    			if (methods[j].isAnnotationPresent(Property.class) && isSetPropertyMethod(methods[j])) {
    				String propertyName = PropertyHelper.getPropertyName(methods[j]) ;
    				String propertyValue = properties.getProperty(propertyName) ;
    				if (log.isDebugEnabled())
    					log.debug("Processing InetAddressInfo for property "+ propertyName+" with value " +propertyValue );
    				if (propertyValue != null && InetAddressInfo.isInetAddressRelated(methods[j])) {
    					Object converted = null ;
						try {
							converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties, propertyValue);
						}
						catch(Exception e) {
							throw new Exception("String value could not be converted for method " + propertyName + " in "
									+ protocolName + " with default value " + propertyValue + ".Exception is " +e, e);
						}
	    				if (log.isDebugEnabled())
	    					log.debug("Adding InetAddressInfo for property "+propertyName+" with value " +propertyValue );
    					InetAddressInfo inetinfo = new InetAddressInfo(protocol, methods[j], properties, propertyValue, converted) ;
    					protocolInetAddressMap.put(propertyName, inetinfo) ; 
    				} // recompute
    			}
    		}

    		//traverse class hierarchy and find all annotated fields and add them to the list if annotated
    		for(Class<?> clazz=protocol.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
    			Field[] fields=clazz.getDeclaredFields();
    			for(int j = 0; j < fields.length; j++ ) {
    				if (fields[j].isAnnotationPresent(Property.class)) {
     					String propertyName = PropertyHelper.getPropertyName(fields[j], properties) ;
    					String propertyValue = properties.getProperty(propertyName) ;
        				if (log.isDebugEnabled())
        					log.debug("Processing InetAddressInfo for property "+propertyName+" with value " +propertyValue );
    					if ((propertyValue != null || !PropertyHelper.usesDefaultConverter(fields[j])) 
    							&& InetAddressInfo.isInetAddressRelated(fields[j])) {
    						Object converted = null ;
							try {
								converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties, propertyValue);
							}
							catch(Exception e) {
								throw new Exception("String value could not be converted for method " + propertyName + " in "
										+ protocolName + " with default value " + propertyValue + ".Exception is " +e, e);
							}
		    				if (log.isDebugEnabled())
		    					log.debug("Adding InetAddressInfo for property "+propertyName+" with value " +propertyValue );
    						InetAddressInfo inetinfo = new InetAddressInfo(protocol, fields[j], properties, propertyValue, converted) ;
    						protocolInetAddressMap.put(propertyName, inetinfo) ; 
    					}// recompute
    				}
    			}
    		}	
    	}

    	// print out maps for debugging
    	if (log.isDebugEnabled()) {
    		for (Map.Entry<String,Map<String, InetAddressInfo>> inetAddressMapEntry : inetAddressMap.entrySet()) {
    			String protocol = inetAddressMapEntry.getKey() ;
    			Map<String, InetAddressInfo> protocolInetAddressMap = inetAddressMapEntry.getValue() ; 

    			log.debug("***InetAddressInfo for protocol: " + protocol + "***") ;
    			for (Map.Entry<String, InetAddressInfo> protocolInetAddressMapEntry : protocolInetAddressMap.entrySet()) {
    				String propertyName = protocolInetAddressMapEntry.getKey() ;
    				InetAddressInfo inetAddressInfo = protocolInetAddressMapEntry.getValue() ; 
    				log.debug("propertyName=" + propertyName + ", addrinfo=" + inetAddressInfo.toString()) ;
    			}
    		}
    	}
    	return inetAddressMap ;
    }
    
    
    /*
     * Method which processes @Property.default() values, associated with the annotation
     * using the defaultValue= attribute. This method does the following:
     * - locate all properties which have no user value assigned
     * - if the defaultValue attribute is not "", generate a value for the field using the 
     * property converter for that property and assign it to the field
     */
    public static void processDefaultValues(Vector<ProtocolConfiguration> protocol_configs, Vector<Protocol> protocols, boolean assumeIPv4) throws Exception {

    	for (int i = 0; i < protocol_configs.size(); i++) {    		
    		ProtocolConfiguration protocol_config = protocol_configs.get(i) ;
    		Protocol protocol = protocols.get(i) ;
    		String protocolName = protocol.getName();

    		// regenerate the Properties which were destroyed during basic property processing
    		Properties properties = null ;
    		properties = protocol_config.getOriginalProperties(); 

    		// check which properties are null
    		Method[] methods=protocol.getClass().getMethods();
    		for(int j = 0; j < methods.length; j++) {
    			if (methods[j].isAnnotationPresent(Property.class) && isSetPropertyMethod(methods[j])) {
    				String propertyName = PropertyHelper.getPropertyName(methods[j]) ;
    				String propertyValue = properties.getProperty(propertyName) ;
    				// if propertyValue is null, check if there is a default value we can assign
    				if (propertyValue == null) {
    					Property annotation = methods[j].getAnnotation(Property.class) ;
    					
    					// get the default value for the method- check for InetAddress types
    					String defaultValue = null ;
    					if (InetAddressInfo.isInetAddressRelated(methods[j])) {
    						if (assumeIPv4)
    							defaultValue = annotation.defaultValueIPv4() ;
    						else
    							defaultValue = annotation.defaultValueIPv6() ;
    					}
    					else {
    						defaultValue = annotation.defaultValue() ;
    					} 
    					// got the default value
    					if (defaultValue != null && defaultValue.length() > 0) {
    						if (log.isDebugEnabled()) 
    							log.debug("Setting default value for property " + propertyName) ;
    						Object converted=null;
    						try {
    							converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties, defaultValue);
    							methods[j].invoke(protocol, converted);
    						}
    						catch(Exception e) {
    							throw new Exception("Deafult could not be assined for method " + propertyName + " in "
    									+ protocolName + " with default value " + defaultValue + ".Exception is " +e, e);
    						}
    						if (log.isDebugEnabled()) {
    							if (converted != null)
    								log.debug("Set property " + propertyName + " to default value " + converted.toString()) ;
    							else 
    								log.debug("Set property " + propertyName + " to default value null") ;
    						}
    					}
    				}
    			}
    		} // for methods

    		//traverse class hierarchy and find all annotated fields and add them to the list if annotated
    		for(Class<?> clazz=protocol.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
    			Field[] fields=clazz.getDeclaredFields();
    			for(int j = 0; j < fields.length; j++ ) {
    				if (fields[j].isAnnotationPresent(Property.class)) {
    					String propertyName = PropertyHelper.getPropertyName(fields[j], properties) ;
    					String propertyValue = properties.getProperty(propertyName) ;
    					if (propertyValue == null) {
    						// add to collection of @Properties with no user specified value
    						Property annotation = fields[j].getAnnotation(Property.class) ;
    						
        					// get the default value for the field - check for InetAddress types
        					String defaultValue = null ;
        					if (InetAddressInfo.isInetAddressRelated(fields[j])) {
        						if (assumeIPv4)
        							defaultValue = annotation.defaultValueIPv4() ;
        						else
        							defaultValue = annotation.defaultValueIPv6() ;
        					}
        					else {
        						defaultValue = annotation.defaultValue() ;
        					} 
        					// got the default value    
    						
        					// set a default value for the field
    						if(defaultValue != null && defaultValue.length() > 0) {
    							// condition for invoking converter
    							if(defaultValue != null || !PropertyHelper.usesDefaultConverter(fields[j])) {
    	    						if (log.isDebugEnabled()) 
    	    							log.debug("Setting default value for property " + propertyName) ;

    								Object converted=null;
    								try {
    									converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties, defaultValue);
    									if(converted != null)
    										setField(fields[j], protocol, converted);
    								}
    								catch(Exception e) {
    									throw new Exception("Deafult could not be assined for field " + propertyName + " in "
    											+ protocolName + " with default value " + defaultValue + ".Exception is " +e, e);
    								}
    								
            						if (log.isDebugEnabled()) {
            							if (converted != null)
            								log.debug("Set property " + propertyName + " to default value " + converted.toString()) ;
            							else 
            								log.debug("Set property " + propertyName + " to default value null") ;
            						}
    							}
    						}
    					}
    				}
    			}
    		} // for fields
    	}
    }
    

    /** Check whether any of the protocols 'below' provide evt_type */
    static boolean providesUpServices(Vector<ProtocolReq> req_list, int evt_type) {        
        for (ProtocolReq pr:req_list){
            if(pr.providesUpService(evt_type))
                return true;
        }
        return false;              
    }


    /** Checks whether any of the protocols 'above' provide evt_type */
    static boolean providesDownServices(Vector<ProtocolReq> req_list, int evt_type) {
        for (ProtocolReq pr:req_list){
            if(pr.providesDownService(evt_type))
                return true;
        }
        return false;
    }

    /**
     * This method creates a list of all properties (Field or Method) in dependency order, 
     * where dependencies are specified using the dependsUpon specifier of the Property annotation. 
     * In particular, it does the following:
     * (i) creates a master list of properties 
     * (ii) checks that all dependency references are present
     * (iii) creates a copy of the master list in dependency order
     */
    static AccessibleObject[] computePropertyDependencies(Object obj, Properties properties) {
    	
    	// List of Fields and Methods of the protocol annotated with @Property
    	List<AccessibleObject> unorderedFieldsAndMethods = new LinkedList<AccessibleObject>() ;
    	List<AccessibleObject> orderedFieldsAndMethods = new LinkedList<AccessibleObject>() ;
    	// Maps property name to property object
    	Map<String, AccessibleObject> propertiesInventory = new HashMap<String, AccessibleObject>() ;
    	
    	// get the methods for this class and add them to the list if annotated with @Property
    	Method[] methods=obj.getClass().getMethods();
    	for(int i = 0; i < methods.length; i++) {
 
    		if (methods[i].isAnnotationPresent(Property.class) && isSetPropertyMethod(methods[i])) {
    			String propertyName = PropertyHelper.getPropertyName(methods[i]) ;
    			unorderedFieldsAndMethods.add(methods[i]) ;
    			propertiesInventory.put(propertyName, methods[i]) ;
    		}
    	}
    	//traverse class hierarchy and find all annotated fields and add them to the list if annotated
    	for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
    		Field[] fields=clazz.getDeclaredFields();
    		for(int i = 0; i < fields.length; i++ ) {
    			if (fields[i].isAnnotationPresent(Property.class)) {
         			String propertyName = PropertyHelper.getPropertyName(fields[i], properties) ;
    				unorderedFieldsAndMethods.add(fields[i]) ;
    				// may need to change this based on name parameter of Property
    				propertiesInventory.put(propertyName, fields[i]) ;
    			}
    		}
    	}
    	    	
//    	if (log.isDebugEnabled()) {
//    		log.debug("Properties inventory keyset for protocol: " + obj.getClass().getName());
//    		Set<String> keyset = propertiesInventory.keySet();
//    		for (Iterator<String> i = keyset.iterator(); i.hasNext();) {
//    			String property = i.next() ;
//    			log.debug("property = " + property);
//    		}
//    	}    	
    	
    	// at this stage, we have all Fields and Methods annotated with @Property
    	checkDependencyReferencesPresent(unorderedFieldsAndMethods, propertiesInventory) ;
    	
    	// order the fields and methods by dependency
    	orderedFieldsAndMethods = orderFieldsAndMethodsByDependency(unorderedFieldsAndMethods, propertiesInventory) ;
    	
//    	if (log.isDebugEnabled()) {
//    		log.debug("Ordered Fields and Methods for protocol: " + obj.getClass().getName());
//    		for (int i = 0; i < orderedFieldsAndMethods.size(); i++) {
//    			AccessibleObject ao = orderedFieldsAndMethods.get(i) ;
//    			log.debug("name = " + ao.toString());
//    		}
//    	}
    	
    	// convert to array of Objects
    	AccessibleObject[] result = new AccessibleObject[orderedFieldsAndMethods.size()] ;
    	for(int i = 0; i < orderedFieldsAndMethods.size(); i++) {
    		result[i] = orderedFieldsAndMethods.get(i) ;
    	}
    	
    	return result ;
    }
    
    static List<AccessibleObject> orderFieldsAndMethodsByDependency(List<AccessibleObject> unorderedList, 
    									Map<String, AccessibleObject> propertiesMap) {
    	// Stack to detect cycle in depends relation
    	Stack<AccessibleObject> stack = new Stack<AccessibleObject>() ;
    	// the result list
    	List<AccessibleObject> orderedList = new LinkedList<AccessibleObject>() ;

    	// add the elements from the unordered list to the ordered list
    	// any dependencies will be checked and added first, in recursive manner
    	for(int i = 0; i < unorderedList.size(); i++) {
    		AccessibleObject obj = unorderedList.get(i) ;
    		addPropertyToDependencyList(orderedList, propertiesMap, stack, obj) ;
    	}
    	
    	return orderedList ;
    }
    
    /**
     *  DFS of dependency graph formed by Property annotations and dependsUpon parameter
     *  This is used to create a list of Properties in dependency order
     */
    static void addPropertyToDependencyList(List<AccessibleObject> orderedList, Map<String, AccessibleObject> props, Stack<AccessibleObject> stack, AccessibleObject obj) {
    
    	if (orderedList.contains(obj))
    		return ;
    	
    	if (stack.search(obj) > 0) {
    		throw new RuntimeException("Deadlock in @Property dependency processing") ;
    	}
    	// record the fact that we are processing obj
    	stack.push(obj) ;
    	// process dependencies for this object before adding it to the list
    	Property annotation = obj.getAnnotation(Property.class) ;
    	String dependsClause = annotation.dependsUpon() ;
    	if (log.isDebugEnabled()) { 
    		log.trace("processing: " + obj.toString() + " with dependsUpon: " + dependsClause);
    	}
    	StringTokenizer st = new StringTokenizer(dependsClause, ",") ;
    	while (st.hasMoreTokens()) {
    		String token = st.nextToken().trim();
    		AccessibleObject dep = props.get(token) ;
    		// if null, throw exception 
    		addPropertyToDependencyList(orderedList, props, stack, dep) ;
    	}
    	// indicate we're done with processing dependencies
    	stack.pop() ;
    	// we can now add in dependency order
    	orderedList.add(obj) ;
    }
    
    /*
     * Checks that for every dependency referred, there is a matching property
     */
    static void checkDependencyReferencesPresent(List<AccessibleObject> objects, Map<String, AccessibleObject> props) {
    	
    	// iterate overall properties marked by @Property
    	for(int i = 0; i < objects.size(); i++) {
    		
    		// get the Property annotation
    		AccessibleObject ao = objects.get(i) ;
    		Property annotation = ao.getAnnotation(Property.class) ;
            if (annotation == null) {
            	throw new IllegalArgumentException("@Property annotation is required for checking dependencies;" + 
            			" annotation is missing for Field/Method " + ao.toString()) ;
            }
    		
    		String dependsClause = annotation.dependsUpon() ;
    		if (dependsClause.trim().equals(""))
    			continue ;
    		
    		// split dependsUpon specifier into tokens; trim each token; search for token in list
    		StringTokenizer st = new StringTokenizer(dependsClause, ",") ;
    		while (st.hasMoreTokens()) {
    			String token = st.nextToken().trim() ;
    			
    			// check that the string representing a property name is in the list
    			boolean found = false ;
    			Set<String> keyset = props.keySet();
    			for (Iterator<String> iter = keyset.iterator(); iter.hasNext();) {
    				if (iter.next().equals(token)) {
    					found = true ;
    					break ;
    				}
    			}
    			if (!found) {
    				throw new IllegalArgumentException("@Property annotation " + annotation.name() + 
    						" has an unresolved dependsUpon property: " + token) ;
    			}
    		}
    	}
    	
    }
    
    public static void resolveAndInvokePropertyMethods(Object obj, Properties props) throws Exception {
        Method[] methods=obj.getClass().getMethods();
        for(Method method: methods) {
        	resolveAndInvokePropertyMethod(obj, method, props) ;
        }
    }

    public static void resolveAndInvokePropertyMethod(Object obj, Method method, Properties props) throws Exception {
    	String methodName=method.getName();
    	if(method.isAnnotationPresent(Property.class) && isSetPropertyMethod(method)) {
    		String propertyName=PropertyHelper.getPropertyName(method) ;
    		String propertyValue=props.getProperty(propertyName);
    		if(propertyValue != null) {
    			Object converted=null;
    			try {
    				converted=PropertyHelper.getConvertedValue(obj, method, props, propertyValue);
    				method.invoke(obj, converted);
    			}
    			catch(Exception e) {
    				String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
    				throw new Exception("Could not assign property " + propertyName + " in "
    						+ name + ", method is " + methodName + ", converted value is " + converted, e);
    			}
    			finally {
    				props.remove(propertyName);
    			}
    		}
    	}
    }
    
    public static void resolveAndAssignFields(Object obj, Properties props) throws Exception {
        //traverse class hierarchy and find all annotated fields
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(Field field: fields) {
            	resolveAndAssignField(obj, field, props) ;
            }
        }
    }

    public static void resolveAndAssignField(Object obj, Field field, Properties props) throws Exception {

    	if(field.isAnnotationPresent(Property.class)) {
    		String propertyName = PropertyHelper.getPropertyName(field, props) ;
    		String propertyValue=props.getProperty(propertyName);
    		
    		if(propertyValue != null || !PropertyHelper.usesDefaultConverter(field)){
    			Object converted=null;
    			try {
    				converted=PropertyHelper.getConvertedValue(obj, field, props, propertyValue);
    				if(converted != null)
    					setField(field, obj, converted);
    			}
    			catch(Exception e) {
    				String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
    				throw new Exception("Property assignment of " + propertyName + " in "
    						+ name + " with original property value " + propertyValue + " and converted to " + converted 
    						+ " could not be assigned. Exception is " +e, e);
    			}
    			finally {
    				props.remove(propertyName);
    			}
    		}
    	}
    }
    
    
    public static void removeDeprecatedProperties(Object obj, Properties props) throws Exception {
        //traverse class hierarchy and find all deprecated properties
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            if(clazz.isAnnotationPresent(DeprecatedProperty.class)) {
                DeprecatedProperty declaredAnnotation=clazz.getAnnotation(DeprecatedProperty.class);
                String[] deprecatedProperties=declaredAnnotation.names();
                for(String propertyName : deprecatedProperties) {
                    String propertyValue=props.getProperty(propertyName);
                    if(propertyValue != null) {
                        if(log.isWarnEnabled()) {
                            String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
                            log.warn(name + " property " + propertyName + " was deprecated and is ignored");
                        }
                        props.remove(propertyName);
                    }
                }
            }
        }
    }

    public static boolean isSetPropertyMethod(Method method) {
        return (method.getName().startsWith("set") &&
                method.getReturnType() == java.lang.Void.TYPE &&
                method.getParameterTypes().length == 1);
    }

    public static String renameFromJavaCodingConvention(String fieldName) {
        Pattern p=Pattern.compile("[A-Z]");
        Matcher m=p.matcher(fieldName.substring(1));
        StringBuffer sb=new StringBuffer();
        while(m.find()) {
            m.appendReplacement(sb, "_" + fieldName.substring(m.end(), m.end() + 1).toLowerCase());
        }
        m.appendTail(sb);
        sb.insert(0, fieldName.substring(0, 1).toLowerCase());
        return sb.toString();
    }

    public static void setField(Field field, Object target, Object value) {
        if(!Modifier.isPublic(field.getModifiers())) {
            field.setAccessible(true);
        }
        try {
            field.set(target, value);
        }
        catch(IllegalAccessException iae) {
            throw new IllegalArgumentException("Could not set field " + field, iae);
        }
    }

    public static Object getField(Field field, Object target) {
        if(!Modifier.isPublic(field.getModifiers())) {
            field.setAccessible(true);
        }
        try {
            return field.get(target);
        }
        catch(IllegalAccessException iae) {
            throw new IllegalArgumentException("Could not get field " + field, iae);
        }
    }


    /* --------------------------- End of Private Methods ---------------------------------- */




    private static class ProtocolReq {
        final Vector<Integer> up_reqs=new Vector<Integer>();
        final Vector<Integer> down_reqs=new Vector<Integer>();
        final Vector<Integer> up_provides=new Vector<Integer>();
        final Vector<Integer> down_provides=new Vector<Integer>();
        final String name;

        ProtocolReq(Protocol p) {
            this.name=p.getName();
            if(p.requiredUpServices() != null) {
                up_reqs.addAll(p.requiredUpServices());
            }
            if(p.requiredDownServices() != null) {
                down_reqs.addAll(p.requiredDownServices());
            }

            if(p.providedUpServices() != null) {
                up_provides.addAll(p.providedUpServices());
            }
            if(p.providedDownServices() != null) {
                down_provides.addAll(p.providedDownServices());
            }

        }

        boolean providesUpService(int evt_type) {
            for(Integer type:up_provides) {
                if(type == evt_type)
                    return true;
            }
            return false;
        }

        boolean providesDownService(int evt_type) {

            for(Integer type:down_provides) {
                if(type == evt_type)
                    return true;
            }
            return false;
        }

        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append('\n' + name + ':');
            if(!up_reqs.isEmpty())
                ret.append("\nRequires from above: " + printUpReqs());

            if(!down_reqs.isEmpty())
                ret.append("\nRequires from below: " + printDownReqs());

            if(!up_provides.isEmpty())
                ret.append("\nProvides to above: " + printUpProvides());

            if(!down_provides.isEmpty())
                ret.append("\nProvides to below: ").append(printDownProvides());
            return ret.toString();
        }

        String printUpReqs() {
            StringBuilder ret;
            ret=new StringBuilder("[");
            for(Integer type:up_reqs) {
                ret.append(Event.type2String(type) + ' ');
            }
            return ret.toString() + ']';
        }

        String printDownReqs() {
            StringBuilder ret=new StringBuilder("[");
            for(Integer type:down_reqs) {
                ret.append(Event.type2String(type) + ' ');
            }
            return ret.toString() + ']';
        }

        String printUpProvides() {
            StringBuilder ret=new StringBuilder("[");
            for(Integer type:up_provides) {
                ret.append(Event.type2String(type) + ' ');
            }
            return ret.toString() + ']';
        }

        String printDownProvides() {
            StringBuilder ret=new StringBuilder("[");
            for(Integer type:down_provides) {
                ret.append(Event.type2String(type) + ' ');
            }
            return ret.toString() + ']';
        }
    }


    /**
     * Parses and encapsulates the specification for 1 protocol of the protocol stack, e.g.
     * <code>UNICAST(timeout=5000)</code>
     */
    public static class ProtocolConfiguration {
        private final String protocol_name;
        private final String properties_str;
        private final Properties properties=new Properties();
        private static final String protocol_prefix="org.jgroups.protocols";


        /**
         * Creates a new ProtocolConfiguration.
         * @param config_str The configuration specification for the protocol, e.g.
         *                   <pre>VERIFY_SUSPECT(timeout=1500)</pre>
         */
        public ProtocolConfiguration(String config_str) throws Exception {
        	
            int index=config_str.indexOf('(');  // e.g. "UDP(in_port=3333)"
            int end_index=config_str.lastIndexOf(')');

            if(index == -1) {
                protocol_name=config_str;
                properties_str="";
            }
            else {
                if(end_index == -1) {
                    throw new Exception("Configurator.ProtocolConfiguration(): closing ')' " +
                            "not found in " + config_str + ": properties cannot be set !");
                }
                else {
                    properties_str=config_str.substring(index + 1, end_index);
                    protocol_name=config_str.substring(0, index);
                }
            }
            parsePropertiesString(properties_str, properties) ;            
        }

        public String getProtocolName() {
            return protocol_name;
        }

        public Properties getProperties() {
            return properties;
        }

        public Properties getOriginalProperties() throws Exception {
        	Properties props = new Properties() ; 
        	
        	parsePropertiesString(properties_str, props) ;
        	return props ;
        }
        
        private void parsePropertiesString(String properties_str, Properties properties) throws Exception {
        	int index = 0 ;
            
        	/* "in_port=5555;out_port=6666" */
            if(properties_str.length() > 0) {
                String[] components=properties_str.split(";");
                for(String property : components) {
                    String name, value;
                    index=property.indexOf('=');
                    if(index == -1) {
                        throw new Exception("Configurator.ProtocolConfiguration(): '=' not found in " + property
                                + " of "
                                + protocol_name);
                    }
                    name=property.substring(0, index);
                    value=property.substring(index + 1, property.length());
                    properties.put(name, value);
                }
            }
        }
        
        private Protocol createLayer(ProtocolStack prot_stack) throws Exception {
            Protocol retval=null;
            if(protocol_name == null)
                return null;

            String defaultProtocolName=protocol_prefix + '.' + protocol_name;
            Class<?> clazz=null;

            try {
                clazz=Util.loadClass(defaultProtocolName, this.getClass());
            }
            catch(ClassNotFoundException e) {
            }

            if(clazz == null) {
                try {
                    clazz=Util.loadClass(protocol_name, this.getClass());
                }
                catch(ClassNotFoundException e) {
                }
                if(clazz == null) {
                    throw new Exception("unable to load class for protocol " + protocol_name +
                            " (either as an absolute - " + protocol_name + " - or relative - " +
                            defaultProtocolName + " - package name)!");
                }
            }

            try {
                retval=(Protocol)clazz.newInstance();
                if(retval == null)
                    throw new Exception("creation of instance for protocol " + protocol_name + "failed !");
                retval.setProtocolStack(prot_stack);
                
                removeDeprecatedProperties(retval, properties);   
                // before processing Field and Method based properties, take dependencies specified
                // with @Property.dependsUpon into account
                AccessibleObject[] dependencyOrderedFieldsAndMethods = computePropertyDependencies(retval, properties) ;
                for(AccessibleObject ordered: dependencyOrderedFieldsAndMethods) {
                	if (ordered instanceof Field) {
                		resolveAndAssignField(retval, (Field)ordered, properties) ;
                	}
                	else if (ordered instanceof Method) {
                		resolveAndInvokePropertyMethod(retval, (Method)ordered, properties) ;                		
                	}
                }
                
                List<Object> additional_objects=retval.getConfigurableObjects();
                if(additional_objects != null && !additional_objects.isEmpty()) {
                    for(Object obj: additional_objects) {
                        resolveAndAssignFields(obj, properties);
                        resolveAndInvokePropertyMethods(obj, properties);
                    }
                }


                if(!properties.isEmpty()) {
                    throw new IllegalArgumentException("the following properties in " + protocol_name
                            + " are not recognized: " + properties);
                }
            }
            catch(InstantiationException inst_ex) {
                log.error("an instance of " + protocol_name + " could not be created. Please check that it implements" +
                        " interface Protocol and that is has a public empty constructor !");
                throw inst_ex;
            }
            return retval;
        }

        public String toString() {
            StringBuilder retval=new StringBuilder();
            retval.append("Protocol: ");
            if(protocol_name == null)
                retval.append("<unknown>");
            else
                retval.append(protocol_name);
            if(properties != null)
                retval.append("(" + properties + ')');
            return retval.toString();
        }
    }
    
    public static class InetAddressInfo {
    	Protocol protocol ;
    	AccessibleObject fieldOrMethod ;
    	Properties properties ;
    	String propertyName ;
    	String stringValue ;
    	Object convertedValue ;
    	boolean isField ;
    	boolean isParameterized ; // is the associated type parametrized? (e.g. Collection<String>)
    	Object baseType ;         // what is the base type (e.g. Collection)

    	InetAddressInfo(Protocol protocol, AccessibleObject fieldOrMethod, Properties properties, String stringValue, 
    			Object convertedValue) {
    		// check input values
    		if (protocol == null) {
    			throw new IllegalArgumentException("Protocol for Field/Method must be non-null") ;
    		}		
    		if (fieldOrMethod instanceof Field) {
    			isField = true ;
    		} else if (fieldOrMethod instanceof Method) {
    			isField = false ;
    		} else 
    			throw new IllegalArgumentException("AccesibleObject is neither Field nor Method") ;
    		if (properties == null) {
    			throw new IllegalArgumentException("Properties for Field/Method must be non-null") ;
    		}		

    		// set the values passed by the user - need to check for null
    		this.protocol = protocol ;
    		this.fieldOrMethod = fieldOrMethod ;
    		this.properties = properties ;
    		this.stringValue = stringValue ;
    		this.convertedValue = convertedValue ;
    		
    		// set the property name
    		Property annotation=fieldOrMethod.getAnnotation(Property.class);    		
    		if (isField())
        		propertyName=PropertyHelper.getPropertyName((Field)fieldOrMethod, properties) ;
    		else 
        		propertyName=PropertyHelper.getPropertyName((Method)fieldOrMethod) ;
    		
    		// is variable type parameterized
    		this.isParameterized = false ;
    		if (isField())
    			this.isParameterized = hasParameterizedType((Field)fieldOrMethod) ;
    		else 
    			this.isParameterized = hasParameterizedType((Method)fieldOrMethod) ;

    		// if parameterized, what is the base type?
    		this.baseType = null ;
    		if (isField() && isParameterized) {
    			// the Field has a single type
    			ParameterizedType fpt = (ParameterizedType)((Field)fieldOrMethod).getGenericType() ;
    			this.baseType = fpt.getActualTypeArguments()[0] ;
    		}
    		else if (!isField() && isParameterized) {
    			// the Method has several parameters (and so types)
    			Type[] types = (Type[])((Method)fieldOrMethod).getGenericParameterTypes();
    			ParameterizedType mpt = (ParameterizedType) types[0] ;
    			this.baseType = mpt.getActualTypeArguments()[0] ;
    		}
    	}

    	// Protocol getProtocol() {return protocol ;}
    	Object getProtocol() {return protocol ;}
    	AccessibleObject getFieldOrMethod() {return fieldOrMethod ;}
    	boolean isField() { return isField ; }
    	String getStringValue() {return stringValue ;}
    	String getPropertyName() {return propertyName ;}
    	Properties getProperties() {return properties ;}
    	Object getConvertedValue() {return convertedValue ;}
    	boolean isParameterized() {return isParameterized ;}
    	Object getBaseType() { return baseType ;}

    	static boolean hasParameterizedType(Field f) {
    		if (f == null) {
    			throw new IllegalArgumentException("Field argument is null") ;
    		}
    		Type type = f.getGenericType();
    		return (type instanceof ParameterizedType) ;
    	}
    	static boolean hasParameterizedType(Method m) throws IllegalArgumentException {
    		if (m == null) {
    			throw new IllegalArgumentException("Method argument is null") ;
    		}
    		Type[] types = m.getGenericParameterTypes() ;
    		return (types[0] instanceof ParameterizedType) ;
    	}
    	static boolean isInetAddressRelated(Field f) {
    		if (hasParameterizedType(f)) {
    			// check for List<InetAddress>, List<InetSocketAddress>, List<IpAddress>
    			ParameterizedType fieldtype = (ParameterizedType) f.getGenericType() ;
    			// check that this parameterized type satisfies our constraints
    			try {
    				parameterizedTypeSanityCheck(fieldtype) ;	
    			}
    			catch(IllegalArgumentException e) {
    				if(log.isDebugEnabled()) {
    					log.debug("Field " + f.getName() + " failed paramaterizedTypeSanityCheck()") ;
    				}
    				// because this Method's parameter fails the sanity check, its probably not 
    				// an InetAddress related structure
    				return false ;
    			}
    			
    			Class<?> listType = (Class<?>) fieldtype.getActualTypeArguments()[0] ;
    			return isInetAddressOrCompatibleType(listType) ;
    		}		
    		else {
    			// check if the non-parameterized type is InetAddress, InetSocketAddress or IpAddress
    			Class<?> fieldtype = f.getType() ;
    			return isInetAddressOrCompatibleType(fieldtype) ;
    		}
    	}
    	/*
    	 * Checks if this method's single parameter represents of of the following:
    	 * an InetAddress, IpAddress or InetSocketAddress or one of 
    	 * List<InetAddress>, List<IpAddress> or List<InetSocketAddress>
    	 */
    	static boolean isInetAddressRelated(Method m) {
    		if (hasParameterizedType(m)) {
    			Type[] types = m.getGenericParameterTypes();
    			ParameterizedType methodParamType = (ParameterizedType)types[0] ;
    			// check that this parameterized type satisfies our constraints
    			try {
    				parameterizedTypeSanityCheck(methodParamType) ;
    			}
    			catch(IllegalArgumentException e) {
    				if(log.isDebugEnabled()) {
    					log.debug("Method " + m.getName() + " failed paramaterizedTypeSanityCheck()") ;
    				}
    				// because this Method's parameter fails the sanity check, its probably not 
    				// an InetAddress related structure
    				return false ;
    			}
    			
    			Class<?> listType = (Class<?>) methodParamType.getActualTypeArguments()[0] ;
    			return isInetAddressOrCompatibleType(listType) ;    			
    		}
    		else {
    			Class<?> methodParamType = m.getParameterTypes()[0] ;
    			return isInetAddressOrCompatibleType(methodParamType) ;
    		}
    	}
    	static boolean isInetAddressOrCompatibleType(Class<?> c) {
     		return c.equals(InetAddress.class) || c.equals(InetSocketAddress.class) || c.equals(InetSocketAddress.class) ;
    	}
    	/*
    	 * Check if the parameterized type represents one of:
    	 * List<InetAddress>, List<IpAddress>, List<InetSocketAddress>
    	 */
    	static void parameterizedTypeSanityCheck(ParameterizedType pt) throws IllegalArgumentException {

    		Type rawType = pt.getRawType() ;
    		Type[] actualTypes = pt.getActualTypeArguments() ;

    		if (log.isDebugEnabled()) {
    			log.debug("sanity check: rawtype = " + rawType.toString()) ;
    			log.debug("sanity check: actualtypes = " + actualTypes.length) ;
    		}
    			
    		// constraints on use of parameterized types with @Property
    		if (!(rawType instanceof Class<?> && rawType.equals(List.class))) {
    			throw new IllegalArgumentException("Invalid parameterized type definition - parameterized type must be a List") ;
    		}
    		// check for non-parameterized type in List
    		if (!(actualTypes[0] instanceof Class<?>)) {
    			throw new IllegalArgumentException("Invalid parameterized type - List must not contain a parameterized type") ;
    		}
    	}

    	/*
    	 * Converts the computedValue to a list of InetAddresses. 
    	 * Because computedValues may be null, we need to return 
    	 * a zero length list in some cases.
    	 */
    	public List<InetAddress> getInetAddresses() {
    		List<InetAddress> addresses = new ArrayList<InetAddress>() ;
    		if (getConvertedValue() == null)
    			return addresses ;
    		// if we take only an InetAddress argument
    		if (!isParameterized()) {
    			addresses.add(getInetAddress(getConvertedValue())) ;
    			return addresses ;
    		}
    		// if we take a List<InetAddress> or similar
    		else {
    			List<?> values = (List<?>) getConvertedValue() ;
    			if (values.size() == 0)
    				return addresses ;
    			for (int i = 0; i < values.size(); i++) {
    				addresses.add(getInetAddress(values.get(i))) ;
    			}
    			return addresses ;
    		}
    	}

    	private InetAddress getInetAddress(Object obj) throws IllegalArgumentException {
    		if (obj == null)
    			throw new IllegalArgumentException("Input argument must represent a non-null IP address") ;
    		if (obj instanceof InetAddress) 
    			return (InetAddress) obj ;
    		else if (obj instanceof IpAddress) 
    			return ((IpAddress) obj).getIpAddress() ;
    		else if (obj instanceof InetSocketAddress)
    			return ((InetSocketAddress) obj).getAddress() ;
    		else {
    			if (log.isDebugEnabled()) 
    				log.debug("Input argument does not represent one of InetAddress...: class=" + obj.getClass().getName()) ;
       			throw new IllegalArgumentException("Input argument does not represent one of InetAddress. IpAddress not InetSocketAddress") ;    			
    		}
     	}
    	
    	public String toString() {
    		StringBuilder sb = new StringBuilder() ;
    		sb.append("InetAddressInfo(") ;
    		sb.append("protocol=" + protocol.getName()) ;
    		sb.append(", propertyName=" + getPropertyName()) ;
    		sb.append(", string value=" + getStringValue()) ;
    		sb.append(", parameterized=" + isParameterized()) ;
    		if (isParameterized())
    			sb.append(", baseType=" + getBaseType()) ;
    		sb.append(")") ;
    		return sb.toString();
    	}
    }


}