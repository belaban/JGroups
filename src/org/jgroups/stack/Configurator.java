package org.jgroups.stack;


import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyHelper;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.util.StackType;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentMap;


/**
 * The task if this class is to setup and configure the protocol stack. A string describing
 * the desired setup, which is both the layering and the configuration of each layer, is
 * given to the configurator which creates and configures the protocol stack and returns
 * a reference to the top layer (Protocol).<p>
 * Future functionality will include the capability to dynamically modify the layering
 * of the protocol stack and the properties of each layer.
 * @author Bela Ban
 * @author Richard Achmatowicz
 */
public class Configurator {
    protected static final Log log=LogFactory.getLog(Configurator.class);
    private final ProtocolStack stack;
    
     
    public Configurator() {      
        stack=null;
    }

    public Configurator(ProtocolStack protocolStack) {
         stack=protocolStack;
    }

    public Protocol setupProtocolStack(List<ProtocolConfiguration> config) throws Exception {
         return setupProtocolStack(config, stack);
    }
     
    public Protocol setupProtocolStack(ProtocolStack copySource) throws Exception {
        List<Protocol> protocols=copySource.copyProtocols(stack);
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
    private static Protocol setupProtocolStack(List<ProtocolConfiguration> protocol_configs, ProtocolStack st) throws Exception {
        List<Protocol> protocols=createProtocols(protocol_configs, st);
        if(protocols == null)
            return null;

        
        // check InetAddress related features of stack
        Map<String, Map<String,InetAddressInfo>> inetAddressMap = createInetAddressMap(protocol_configs, protocols) ;
        Collection<InetAddress> addrs=getAddresses(inetAddressMap);

        StackType ip_version=Util.getIpStackType(); // 0 = n/a, 4 = IPv4, 6 = IPv6

        if(!addrs.isEmpty()) {
            // check that all user-supplied InetAddresses have a consistent version:
            // 1. If an addr is IPv6 and we have an IPv4 stack --> FAIL
            // 2. If an address is an IPv4 class D (multicast) address and the stack is IPv6: FAIL
            // Else pass

            for(InetAddress addr: addrs) {
                if(addr instanceof Inet6Address && ip_version == StackType.IPv4)
                    throw new IllegalArgumentException("found IPv6 address " + addr + " in an IPv4 stack");
                if(addr instanceof Inet4Address && addr.isMulticastAddress() && ip_version == StackType.IPv6)
                    throw new Exception("found IPv4 multicast address " + addr + " in an IPv6 stack");
            }
        }


        // process default values
        setDefaultValues(protocol_configs, protocols, ip_version);
        ensureValidBindAddresses(protocols);
        return connectProtocols(protocols);
    }


    public static void setDefaultValues(List<Protocol> protocols) throws Exception {
        if(protocols == null)
            return;

        // check InetAddress related features of stack
        Collection<InetAddress> addrs=getInetAddresses(protocols);
        StackType ip_version=Util.getIpStackType(); // 0 = n/a, 4 = IPv4, 6 = IPv6

        if(!addrs.isEmpty()) {
            // check that all user-supplied InetAddresses have a consistent version:
            // 1. If an addr is IPv6 and we have an IPv4 stack --> FAIL
            // 2. If an address is an IPv4 class D (multicast) address and the stack is IPv6: FAIL
            // Else pass

            for(InetAddress addr : addrs) {
                if(addr instanceof Inet6Address && ip_version == StackType.IPv4)
                    throw new IllegalArgumentException("found IPv6 address " + addr + " in an IPv4 stack");
                if(addr instanceof Inet4Address && addr.isMulticastAddress() && ip_version == StackType.IPv6)
                    throw new Exception("found IPv4 multicast address " + addr + " in an IPv6 stack");
            }
        }

        // process default values
        setDefaultValues(protocols, ip_version);
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
        prot=createLayer(stack, config);
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
    public static Protocol connectProtocols(List<Protocol> protocol_list) throws Exception {
        Protocol current_layer=null, next_layer=null;

        for(int i=0; i < protocol_list.size(); i++) {
            current_layer=protocol_list.get(i);
            if(i + 1 >= protocol_list.size())
                break;
            next_layer=protocol_list.get(i + 1);
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

        // basic protocol sanity check
        sanityCheck(protocol_list);

        return current_layer;
    }


    /**
     * Get a string of the form "P1(config_str1):P2:P3(config_str3)" and return
     * ProtocolConfigurations for it. That means, parse "P1(config_str1)", "P2" and
     * "P3(config_str3)"
     * @param config_str Configuration string
     * @return Vector of strings
     */
    private static List<String> parseProtocols(String config_str) throws IOException {
        List<String> retval=new LinkedList<String>();
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
     * @return List of ProtocolConfigurations
     */
    public static List<ProtocolConfiguration> parseConfigurations(String configuration) throws Exception {
        List<ProtocolConfiguration> retval=new ArrayList<ProtocolConfiguration>();
        List<String> protocol_string=parseProtocols(configuration);

        if(protocol_string == null)
            return null;
        
        for(String component_string: protocol_string) {                       
            retval.add(new ProtocolConfiguration(component_string));
        }
        return retval;
    }


    public static String printConfigurations(Collection<ProtocolConfiguration> configs) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(ProtocolConfiguration config: configs) {
            if(first)
                first=false;
            else
                sb.append(":");
            sb.append(config.getProtocolName());
            if(!config.getProperties().isEmpty()) {
                sb.append('(').append(config.propertiesToString()).append(')');
            }
        }

        return sb.toString();
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
     * @return List of Protocols
     */
    private static List<Protocol> createProtocols(List<ProtocolConfiguration> protocol_configs, final ProtocolStack stack) throws Exception {
        List<Protocol> retval=new LinkedList<Protocol>();
        ProtocolConfiguration protocol_config;
        Protocol layer;
        String singleton_name;

        for(int i=0; i < protocol_configs.size(); i++) {
            protocol_config=protocol_configs.get(i);
            singleton_name=protocol_config.getProperties().get(Global.SINGLETON_NAME);
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
                        layer=createLayer(stack, protocol_config);
                        if(layer == null)
                            return null;
                        singleton_transports.put(singleton_name, new Tuple<TP, ProtocolStack.RefCounter>((TP)layer,new ProtocolStack.RefCounter((short)0,(short)0)));
                        retval.add(layer);
                    }
                }
                continue;
            }
            layer=createLayer(stack, protocol_config);
            if(layer == null)
                return null;
            retval.add(layer);
        }
        return retval;
    }


    protected static Protocol createLayer(ProtocolStack stack, ProtocolConfiguration config) throws Exception {
        String              protocol_name=config.getProtocolName();
        Map<String, String> properties=config.getProperties();
        Protocol            retval=null;

        if(protocol_name == null || properties == null)
            return null;

        String defaultProtocolName=ProtocolConfiguration.protocol_prefix + '.' + protocol_name;
        Class<?> clazz=null;

        try {
            clazz=Util.loadClass(defaultProtocolName, stack.getClass());
        }
        catch(ClassNotFoundException e) {
        }

        if(clazz == null) {
            try {
                clazz=Util.loadClass(protocol_name, stack.getClass());
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
            retval.setProtocolStack(stack);

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


    /**
     Throws an exception if sanity check fails. Possible sanity check is uniqueness of all protocol names
     */
    public static void sanityCheck(List<Protocol> protocols) throws Exception {

        // check for unique IDs
        Set<Short> ids=new HashSet<Short>();
        for(Protocol protocol: protocols) {
            short id=protocol.getId();
            if(id > 0 && ids.add(id) == false)
                throw new Exception("Protocol ID " + id + " (name=" + protocol.getName() +
                        ") is duplicate; protocol IDs have to be unique");
        }


        // For each protocol, get its required up and down services and check (if available) if they're satisfied
        for(Protocol protocol: protocols) {
            List<Integer> required_down_services=protocol.requiredDownServices();
            List<Integer> required_up_services=protocol.requiredUpServices();

            if(required_down_services != null && !required_down_services.isEmpty()) {

                // the protocols below 'protocol' have to provide the services listed in required_down_services
                List<Integer> tmp=new ArrayList<Integer>(required_down_services);
                removeProvidedUpServices(protocol, tmp);
                if(!tmp.isEmpty())
                    throw new Exception("events " + printEvents(tmp) + " are required by " + protocol.getName() +
                                          ", but not provided by any of the protocols below it");
            }

            if(required_up_services != null && !required_up_services.isEmpty()) {

                // the protocols above 'protocol' have to provide the services listed in required_up_services
                List<Integer> tmp=new ArrayList<Integer>(required_up_services);
                removeProvidedDownServices(protocol, tmp);
                if(!tmp.isEmpty())
                    throw new Exception("events " + printEvents(tmp) + " are required by " + protocol.getName() +
                                          ", but not provided by any of the protocols above it");
            }
        }
    }

    protected static String printEvents(List<Integer> events) {
        StringBuilder sb=new StringBuilder("[");
        for(int evt: events)
            sb.append(Event.type2String(evt)).append(" ");
        sb.append("]");
        return sb.toString();
    }

    /**
     * Removes all events provided by the protocol below protocol from events
     * @param protocol
     * @param events
     */
    protected static void removeProvidedUpServices(Protocol protocol, List<Integer> events) {
        if(protocol == null || events == null)
            return;
        for(Protocol prot=protocol.getDownProtocol(); prot != null && !events.isEmpty(); prot=prot.getDownProtocol()) {
            List<Integer> provided_up_services=prot.providedUpServices();
            if(provided_up_services != null && !provided_up_services.isEmpty())
                events.removeAll(provided_up_services);
        }
    }

      /**
     * Removes all events provided by the protocol above protocol from events
     * @param protocol
     * @param events
     */
    protected static void removeProvidedDownServices(Protocol protocol, List<Integer> events) {
        if(protocol == null || events == null)
            return;
        for(Protocol prot=protocol.getUpProtocol(); prot != null && !events.isEmpty(); prot=prot.getUpProtocol()) {
            List<Integer> provided_down_services=prot.providedDownServices();
            if(provided_down_services != null && !provided_down_services.isEmpty())
                events.removeAll(provided_down_services);
        }
    }


    /**
     * Returns all inet addresses found
     */
    public static Collection<InetAddress> getAddresses(Map<String, Map<String, InetAddressInfo>> inetAddressMap) throws Exception {
        Set<InetAddress> addrs=new HashSet<InetAddress>();

        for(Map.Entry<String, Map<String, InetAddressInfo>> inetAddressMapEntry : inetAddressMap.entrySet()) {
            Map<String, InetAddressInfo> protocolInetAddressMap=inetAddressMapEntry.getValue();
            for(Map.Entry<String, InetAddressInfo> protocolInetAddressMapEntry : protocolInetAddressMap.entrySet()) {
                InetAddressInfo inetAddressInfo=protocolInetAddressMapEntry.getValue();
                // add InetAddressInfo to sets based on IP version
                List<InetAddress> addresses=inetAddressInfo.getInetAddresses();
                for(InetAddress address : addresses) {
                    if(address == null)
                        throw new RuntimeException("failed converting address info to IP address: " + inetAddressInfo);
                    addrs.add(address);
                }
            }
        }
        return addrs;
    }


    /**
     * This method takes a set of InetAddresses, represented by an inetAddressmap, and:
     * - if the resulting set is non-empty, goes through to see if all InetAddress-related
     * user settings have a consistent IP version: v4 or v6, and throws an exception if not
     * - if the resulting set is empty, sets the default IP version based on available stacks
     * and if a dual stack, stack preferences
     * - sets the IP version to be used in the JGroups session
     * @return StackType.IPv4 for IPv4, StackType.IPv6 for IPv6, StackType.Unknown if the version cannot be determined
     */
    public static StackType determineIpVersionFromAddresses(Collection<InetAddress> addrs) throws Exception {
    	Set<InetAddress> ipv4_addrs= new HashSet<InetAddress>() ;
    	Set<InetAddress> ipv6_addrs= new HashSet<InetAddress>() ;

        for(InetAddress address: addrs) {
            if (address instanceof Inet4Address)
                ipv4_addrs.add(address) ;
            else
                ipv6_addrs.add(address) ;
        }

        if(log.isTraceEnabled())
            log.trace("all addrs=" + addrs + ", IPv4 addrs=" + ipv4_addrs + ", IPv6 addrs=" + ipv6_addrs);

		// the user supplied 1 or more IP address inputs. Check if we have a consistent set
        if (!addrs.isEmpty()) {
            if (!ipv4_addrs.isEmpty() && !ipv6_addrs.isEmpty()) {
                throw new RuntimeException("all addresses have to be either IPv4 or IPv6: IPv4 addresses=" +
                        ipv4_addrs + ", IPv6 addresses=" + ipv6_addrs);
            }
            return !ipv6_addrs.isEmpty()? StackType.IPv6 : StackType.IPv4;
        }
        return StackType.Unknown;
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
    public static Map<String, Map<String,InetAddressInfo>> createInetAddressMap(List<ProtocolConfiguration> protocol_configs,
                                                                                List<Protocol> protocols) throws Exception {
    	// Map protocol -> Map<String, InetAddressInfo>, where the latter is protocol specific
    	Map<String, Map<String,InetAddressInfo>> inetAddressMap = new HashMap<String, Map<String, InetAddressInfo>>() ;

    	// collect InetAddressInfo
    	for (int i = 0; i < protocol_configs.size(); i++) {    		        	
    		ProtocolConfiguration protocol_config = protocol_configs.get(i) ;
    		Protocol protocol = protocols.get(i) ;
    		String protocolName = protocol.getName();

    		// regenerate the Properties which were destroyed during basic property processing
    		Map<String,String> properties = protocol_config.getOriginalProperties();

    		// check which InetAddress-related properties are ***non-null ***, and
    		// create an InetAddressInfo structure for them
    		// Method[] methods=protocol.getClass().getMethods();
            Method[] methods=Util.getAllDeclaredMethodsWithAnnotations(protocol.getClass(), Property.class);
    		for(int j = 0; j < methods.length; j++) {
    			if (methods[j].isAnnotationPresent(Property.class) && isSetPropertyMethod(methods[j])) {
    				String propertyName = PropertyHelper.getPropertyName(methods[j]) ;
    				String propertyValue = properties.get(propertyName);

                    // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
                    String tmp=grabSystemProp(methods[j].getAnnotation(Property.class));
                    if(tmp != null)
                        propertyValue=tmp;

    				if (propertyValue != null && InetAddressInfo.isInetAddressRelated(methods[j])) {
    					Object converted = null ;
						try {
							converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties, propertyValue, false);
						}
						catch(Exception e) {
							throw new Exception("String value could not be converted for method " + propertyName + " in "
									+ protocolName + " with default value " + propertyValue + ".Exception is " +e, e);
						}
    					InetAddressInfo inetinfo = new InetAddressInfo(protocol, methods[j], properties, propertyValue, converted) ;

                        Map<String, InetAddressInfo> protocolInetAddressMap=inetAddressMap.get(protocolName);
                        if(protocolInetAddressMap == null) {
                            protocolInetAddressMap = new HashMap<String,InetAddressInfo>() ;
                            inetAddressMap.put(protocolName, protocolInetAddressMap) ;
                        }
    					protocolInetAddressMap.put(propertyName, inetinfo) ; 
    				}
    			}
    		}

    		//traverse class hierarchy and find all annotated fields and add them to the list if annotated
    		for(Class<?> clazz=protocol.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
    			Field[] fields=clazz.getDeclaredFields();
    			for(int j = 0; j < fields.length; j++ ) {
    				if (fields[j].isAnnotationPresent(Property.class)) {
     					String propertyName = PropertyHelper.getPropertyName(fields[j], properties) ;
    					String propertyValue = properties.get(propertyName) ;

                        // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
                        String tmp=grabSystemProp(fields[j].getAnnotation(Property.class));
                        if(tmp != null)
                            propertyValue=tmp;
                        
    					if ((propertyValue != null || !PropertyHelper.usesDefaultConverter(fields[j]))
    							&& InetAddressInfo.isInetAddressRelated(protocol, fields[j])) {
    						Object converted = null ;
							try {
								converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties, propertyValue, false);
							}
							catch(Exception e) {
								throw new Exception("String value could not be converted for method " + propertyName + " in "
										+ protocolName + " with default value " + propertyValue + ".Exception is " +e, e);
							}
    						InetAddressInfo inetinfo = new InetAddressInfo(protocol, fields[j], properties, propertyValue, converted) ;

                            Map<String, InetAddressInfo> protocolInetAddressMap=inetAddressMap.get(protocolName);
                            if(protocolInetAddressMap == null) {
                                protocolInetAddressMap = new HashMap<String,InetAddressInfo>() ;
                                inetAddressMap.put(protocolName, protocolInetAddressMap) ;
                            }
    						protocolInetAddressMap.put(propertyName, inetinfo) ;
    					}// recompute
    				}
    			}
    		}	
    	}
    	return inetAddressMap ;
    }


    public static List<InetAddress> getInetAddresses(List<Protocol> protocols) throws Exception {
        List<InetAddress> retval=new LinkedList<InetAddress>();

        // collect InetAddressInfo
        for(Protocol protocol : protocols) {
            String protocolName=protocol.getName();

            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            for(Class<?> clazz=protocol.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
                Field[] fields=clazz.getDeclaredFields();
                for(int j=0; j < fields.length; j++) {
                    if(fields[j].isAnnotationPresent(Property.class)) {
                        if(InetAddressInfo.isInetAddressRelated(protocol, fields[j])) {
                            Object value=getValueFromProtocol(protocol, fields[j]);
                            if(value instanceof InetAddress)
                                retval.add((InetAddress)value);
                            else if(value instanceof IpAddress)
                                retval.add(((IpAddress)value).getIpAddress());
                            else if(value instanceof InetSocketAddress)
                                retval.add(((InetSocketAddress)value).getAddress());
                        }
                    }
                }
            }
        }
        return retval;
    }


    /*
    * Method which processes @Property.default() values, associated with the annotation
    * using the defaultValue= attribute. This method does the following:
    * - locate all properties which have no user value assigned
    * - if the defaultValue attribute is not "", generate a value for the field using the
    * property converter for that property and assign it to the field
    */
    public static void setDefaultValues(List<ProtocolConfiguration> protocol_configs, List<Protocol> protocols,
                                        StackType ip_version) throws Exception {
        InetAddress default_ip_address=Util.getNonLoopbackAddress();
        if(default_ip_address == null) {
            log.warn("unable to find an address other than loopback for IP version " + ip_version);
            default_ip_address=Util.getLocalhost(ip_version);
        }

        for(int i=0; i < protocol_configs.size(); i++) {
            ProtocolConfiguration protocol_config=protocol_configs.get(i);
            Protocol protocol=protocols.get(i);
            String protocolName=protocol.getName();

            // regenerate the Properties which were destroyed during basic property processing
            Map<String,String> properties=protocol_config.getOriginalProperties();

            Method[] methods=Util.getAllDeclaredMethodsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < methods.length; j++) {
                if(isSetPropertyMethod(methods[j])) {
                    String propertyName=PropertyHelper.getPropertyName(methods[j]);

                    Object propertyValue=getValueFromProtocol(protocol, propertyName);
                    if(propertyValue == null) { // if propertyValue is null, check if there is a we can use
                        Property annotation=methods[j].getAnnotation(Property.class);

                        // get the default value for the method- check for InetAddress types
                        String defaultValue=null;
                        if(InetAddressInfo.isInetAddressRelated(methods[j])) {
                            defaultValue=ip_version == StackType.IPv4? annotation.defaultValueIPv4() : annotation.defaultValueIPv6();
                            if(defaultValue != null && defaultValue.length() > 0) {
                                Object converted=null;
                                try {
                                    if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                        converted=default_ip_address;
                                    else
                                        converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties, defaultValue, true);
                                    methods[j].invoke(protocol, converted);
                                }
                                catch(Exception e) {
                                    throw new Exception("default could not be assigned for method " + propertyName + " in "
                                            + protocolName + " with default " + defaultValue, e);
                                }
                                if(log.isDebugEnabled())
                                    log.debug("set property " + protocolName + "." + propertyName + " to default value " + converted);
                            }
                        }
                    }
                }
            } 

            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < fields.length; j++) {
                String propertyName=PropertyHelper.getPropertyName(fields[j], properties);
                Object propertyValue=getValueFromProtocol(protocol, fields[j]);
                if(propertyValue == null) {
                    // add to collection of @Properties with no user specified value
                    Property annotation=fields[j].getAnnotation(Property.class);

                    // get the default value for the field - check for InetAddress types
                    String defaultValue=null;
                    if(InetAddressInfo.isInetAddressRelated(protocol, fields[j])) {
                        defaultValue=ip_version == StackType.IPv4? annotation.defaultValueIPv4() : annotation.defaultValueIPv6();
                        if(defaultValue != null && defaultValue.length() > 0) {
                            // condition for invoking converter
                            if(defaultValue != null || !PropertyHelper.usesDefaultConverter(fields[j])) {
                                Object converted=null;
                                try {
                                    if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                        converted=default_ip_address;
                                    else
                                        converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties, defaultValue, true);
                                    if(converted != null)
                                        Util.setField(fields[j], protocol, converted);
                                }
                                catch(Exception e) {
                                    throw new Exception("default could not be assigned for field " + propertyName + " in "
                                            + protocolName + " with default value " + defaultValue, e);
                                }

                                if(log.isDebugEnabled())
                                    log.debug("set property " + protocolName + "." + propertyName + " to default value " + converted);
                            }
                        }
                    }
                }
            }
        }
    }


    public static void setDefaultValues(List<Protocol> protocols, StackType ip_version) throws Exception {
        InetAddress default_ip_address=Util.getNonLoopbackAddress();
        if(default_ip_address == null) {
            log.warn("unable to find an address other than loopback for IP version " + ip_version);
            default_ip_address=Util.getLocalhost(ip_version);
        }

        for(Protocol protocol : protocols) {
            String protocolName=protocol.getName();

            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < fields.length; j++) {
                // get the default value for the field - check for InetAddress types
                if(InetAddressInfo.isInetAddressRelated(protocol, fields[j])) {
                    Object propertyValue=getValueFromProtocol(protocol, fields[j]);
                    if(propertyValue == null) {
                        // add to collection of @Properties with no user specified value
                        Property annotation=fields[j].getAnnotation(Property.class);

                        String defaultValue=ip_version == StackType.IPv4? annotation.defaultValueIPv4() : annotation.defaultValueIPv6();
                        if(defaultValue != null && defaultValue.length() > 0) {
                            // condition for invoking converter
                            Object converted=null;
                            try {
                                if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                    converted=default_ip_address;
                                else
                                    converted=PropertyHelper.getConvertedValue(protocol, fields[j], defaultValue, true);
                                if(converted != null)
                                    Util.setField(fields[j], protocol, converted);
                            }
                            catch(Exception e) {
                                throw new Exception("default could not be assigned for field " + fields[j].getName() + " in "
                                        + protocolName + " with default value " + defaultValue, e);
                            }

                            if(log.isDebugEnabled())
                                log.debug("set property " + protocolName + "." + fields[j].getName() + " to default value " + converted);
                        }
                    }
                }
            }
        }
    }


    /**
     * Makes sure that all fields annotated with @LocalAddress is (1) an InetAddress and (2) a valid address on any
     * local network interface
     * @param protocols
     * @throws Exception
     */
    public static void ensureValidBindAddresses(List<Protocol> protocols) throws Exception {
        for(Protocol protocol : protocols) {
            String protocolName=protocol.getName();

            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(protocol.getClass(), LocalAddress.class);
            for(int i=0; i < fields.length; i++) {
                Object val=getValueFromProtocol(protocol, fields[i]);
                if(val == null)
                    continue;
                if(!(val instanceof InetAddress))
                    throw new Exception("field " + protocolName + "." + fields[i].getName() + " is not an InetAddress");
                Util.checkIfValidAddress((InetAddress)val, protocolName);
            }
        }
    }



    public static Object getValueFromProtocol(Protocol protocol, Field field) throws IllegalAccessException {
        if(protocol == null || field == null) return null;
        if(!Modifier.isPublic(field.getModifiers()))
            field.setAccessible(true);
        return field.get(protocol);
    }


    public static Object getValueFromProtocol(Protocol protocol, String field_name) throws IllegalAccessException {
        if(protocol == null || field_name == null) return null;
        Field field=Util.getField(protocol.getClass(), field_name);
        return field != null? getValueFromProtocol(protocol, field) : null;
    }



    /**
     * This method creates a list of all properties (Field or Method) in dependency order, 
     * where dependencies are specified using the dependsUpon specifier of the Property annotation. 
     * In particular, it does the following:
     * (i) creates a master list of properties 
     * (ii) checks that all dependency references are present
     * (iii) creates a copy of the master list in dependency order
     */
    static AccessibleObject[] computePropertyDependencies(Object obj, Map<String,String> properties) {
    	
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

    	// at this stage, we have all Fields and Methods annotated with @Property
    	checkDependencyReferencesPresent(unorderedFieldsAndMethods, propertiesInventory) ;
    	
    	// order the fields and methods by dependency
    	orderedFieldsAndMethods = orderFieldsAndMethodsByDependency(unorderedFieldsAndMethods, propertiesInventory) ;
    	
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
    		if (dependsClause.trim().length() == 0)
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
    
    public static void resolveAndInvokePropertyMethods(Object obj, Map<String,String> props) throws Exception {
        Method[] methods=obj.getClass().getMethods();
        for(Method method: methods) {
        	resolveAndInvokePropertyMethod(obj, method, props) ;
        }
    }

    public static void resolveAndInvokePropertyMethod(Object obj, Method method, Map<String,String> props) throws Exception {
    	String methodName=method.getName();
        Property annotation=method.getAnnotation(Property.class);
    	if(annotation != null && isSetPropertyMethod(method)) {
    		String propertyName=PropertyHelper.getPropertyName(method) ;
    		String propertyValue=props.get(propertyName);

            // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
            String tmp=grabSystemProp(method.getAnnotation(Property.class));
            if(tmp != null)
                propertyValue=tmp;

            if(propertyName != null && propertyValue != null) {
                String deprecated_msg=annotation.deprecatedMessage();
                if(deprecated_msg != null && deprecated_msg.length() > 0) {
                    log.warn(method.getDeclaringClass().getSimpleName() + "." + methodName + " has been deprecated : " +
                            deprecated_msg);
                }
            }

    		if(propertyValue != null) {
    			Object converted=null;
    			try {
    				converted=PropertyHelper.getConvertedValue(obj, method, props, propertyValue, true);
    				method.invoke(obj, converted);
    			}
    			catch(Exception e) {
    				String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
    				throw new Exception("Could not assign property " + propertyName + " in "
    						+ name + ", method is " + methodName + ", converted value is " + converted, e);
    			}
    		}

            props.remove(propertyName);
    	}
    }
    
    public static void resolveAndAssignFields(Object obj, Map<String,String> props) throws Exception {
        //traverse class hierarchy and find all annotated fields
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(Field field: fields) {
            	resolveAndAssignField(obj, field, props) ;
            }
        }
    }

    public static void resolveAndAssignField(Object obj, Field field, Map<String,String> props) throws Exception {
        Property annotation=field.getAnnotation(Property.class);
    	if(annotation != null) {
    		String propertyName = PropertyHelper.getPropertyName(field, props) ;
    		String propertyValue=props.get(propertyName);

            // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
            String tmp=grabSystemProp(field.getAnnotation(Property.class));
            if(tmp != null)
                propertyValue=tmp;

            if(propertyName != null && propertyValue != null) {
                String deprecated_msg=annotation.deprecatedMessage();
                if(deprecated_msg != null && deprecated_msg.length() > 0) {
                    log.warn(field.getDeclaringClass().getSimpleName() + "." + field.getName() + " has been deprecated: " +
                            deprecated_msg);
                }
            }
            
    		if(propertyValue != null || !PropertyHelper.usesDefaultConverter(field)){
    			Object converted=null;
    			try {
    				converted=PropertyHelper.getConvertedValue(obj, field, props, propertyValue, true);
    				if(converted != null)
    					Util.setField(field, obj, converted);
    			}
    			catch(Exception e) {
    				String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
    				throw new Exception("Property assignment of " + propertyName + " in "
    						+ name + " with original property value " + propertyValue + " and converted to " + converted 
    						+ " could not be assigned", e);
    			}
    		}

            props.remove(propertyName);
    	}
    }
    
    
    public static void removeDeprecatedProperties(Object obj, Map<String,String> props) throws Exception {
        //traverse class hierarchy and find all deprecated properties
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            if(clazz.isAnnotationPresent(DeprecatedProperty.class)) {
                DeprecatedProperty declaredAnnotation=clazz.getAnnotation(DeprecatedProperty.class);
                String[] deprecatedProperties=declaredAnnotation.names();
                for(String propertyName : deprecatedProperties) {
                    String propertyValue=props.get(propertyName);
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


  

    private static String grabSystemProp(Property annotation) {
        String[] system_property_names=annotation.systemProperty();
        String retval=null;

        for(String system_property_name: system_property_names) {
            if(system_property_name != null && system_property_name.length() > 0) {
                if(system_property_name.equals(Global.BIND_ADDR))
                    if(Util.isBindAddressPropertyIgnored())
                        continue;
                
                try {
                    retval=System.getProperty(system_property_name);
                    if(retval != null)
                        return retval;
                }
                catch(SecurityException ex) {
                    log.error("failed getting system property for " + system_property_name, ex);
                }
            }
        }
        return retval;
    }

    /* --------------------------- End of Private Methods ---------------------------------- */





    public static class InetAddressInfo {
    	Protocol protocol ;
    	AccessibleObject fieldOrMethod ;
    	Map<String,String> properties ;
    	String propertyName ;
    	String stringValue ;
    	Object convertedValue ;
    	boolean isField ;
    	boolean isParameterized ; // is the associated type parametrized? (e.g. Collection<String>)
    	Object baseType ;         // what is the base type (e.g. Collection)

    	InetAddressInfo(Protocol protocol, AccessibleObject fieldOrMethod, Map<String,String> properties, String stringValue,
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
    	Map<String,String> getProperties() {return properties ;}
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

    	static boolean isInetAddressRelated(Protocol prot, Field f) {
    		if (hasParameterizedType(f)) {
    			// check for List<InetAddress>, List<InetSocketAddress>, List<IpAddress>
    			ParameterizedType fieldtype = (ParameterizedType) f.getGenericType() ;
    			// check that this parameterized type satisfies our constraints
    			try {
    				parameterizedTypeSanityCheck(fieldtype) ;	
    			}
    			catch(IllegalArgumentException e) {
    				// because this Method's parameter fails the sanity check, its probably not an InetAddress related structure
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
    				if(log.isErrorEnabled()) {
    					log.error("Method " + m.getName() + " failed paramaterizedTypeSanityCheck()") ;
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
     		return c.equals(InetAddress.class) || c.equals(InetSocketAddress.class) || c.equals(IpAddress.class) ;
    	}
    	/*
    	 * Check if the parameterized type represents one of:
    	 * List<InetAddress>, List<IpAddress>, List<InetSocketAddress>
    	 */
    	static void parameterizedTypeSanityCheck(ParameterizedType pt) throws IllegalArgumentException {

    		Type rawType = pt.getRawType() ;
    		Type[] actualTypes = pt.getActualTypeArguments() ;
    			
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
    			if (values.isEmpty())
    				return addresses ;
    			for (int i = 0; i < values.size(); i++) {
    				addresses.add(getInetAddress(values.get(i))) ;
    			}
    			return addresses ;
    		}
    	}

    	private static InetAddress getInetAddress(Object obj) throws IllegalArgumentException {
    		if (obj == null)
    			throw new IllegalArgumentException("Input argument must represent a non-null IP address") ;
    		if (obj instanceof InetAddress) 
    			return (InetAddress) obj ;
    		else if (obj instanceof IpAddress) 
    			return ((IpAddress) obj).getIpAddress() ;
    		else if (obj instanceof InetSocketAddress)
    			return ((InetSocketAddress) obj).getAddress() ;
    		else {
    			if (log.isWarnEnabled())
    				log.warn("Input argument does not represent one of InetAddress...: class=" + obj.getClass().getName()) ;
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
