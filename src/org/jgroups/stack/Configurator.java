package org.jgroups.stack;


import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyHelper;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.w3c.dom.Node;

import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;


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
    protected static final Log    log=LogFactory.getLog(Configurator.class);
    protected final ProtocolStack stack;


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
     * Sets up the protocol stack. Each {@link ProtocolConfiguration} has the protocol name and a map of attribute
     * names and values (strings). Reflection is used to find the right fields (or setters) based on attribute names,
     * and set them (by converting the attribute value to the proper object).
     */
    public static Protocol setupProtocolStack(List<ProtocolConfiguration> protocol_configs, ProtocolStack st) throws Exception {
        List<Protocol> protocols=createProtocolsAndInitializeAttrs(protocol_configs, st);
        // Fixes NPE with concurrent channel creation when using a shared stack (https://issues.jboss.org/browse/JGRP-1488)
        Protocol top_protocol=protocols.get(protocols.size() - 1);
        top_protocol.setUpProtocol(st);
        return connectProtocols(protocols);
    }


    public static Protocol createProtocol(String prot_spec, ProtocolStack stack) throws Exception {
        return createProtocol(prot_spec, stack, true);
    }

    /**
     * Creates a new protocol given the protocol specification. Initializes the properties and starts the
     * up and down handler threads.
     * @param prot_spec The specification of the protocol. Same convention as for specifying a protocol stack.
     *                  An exception will be thrown if the class cannot be created. Example:
     *                  <pre>"VERIFY_SUSPECT(timeout=1500)"</pre> Note that no colons (:) have to be
     *                  specified
     * @param stack     The protocol stack
     * @return Protocol The newly created protocol
     * @throws Exception Will be thrown when the new protocol cannot be created
     */
    public static Protocol createProtocol(String prot_spec, ProtocolStack stack, boolean init_attrs) throws Exception {
        if(prot_spec == null) throw new Exception("Configurator.createProtocol(): prot_spec is null");

        // parse the configuration for this protocol
        ProtocolConfiguration config=new ProtocolConfiguration(prot_spec);

        // create an instance of the protocol class and configure it
        Protocol prot=createLayer(stack, config);
        if(init_attrs)
            Configurator.initializeAttrs(prot, config, Util.getIpStackType());
        prot.init();
        return prot;
    }

    public static List<Protocol> createProtocolsAndInitializeAttrs(List<ProtocolConfiguration> cfgs,
                                                                   ProtocolStack st) throws Exception {
        List<Protocol> protocols=createProtocols(cfgs, st);
        if(protocols == null)
            return null;

        // Determine how to resolve addresses that are set (e.g.) via symbolic names, by the type of bind_addr in the
        // transport. The logic below is described in https://issues.jboss.org/browse/JGRP-2343
        StackType ip_version=Util.getIpStackType();
        Protocol transport=protocols.get(0);
        if(transport instanceof TP) {
            ProtocolConfiguration cfg=cfgs.get(0);
            Field bind_addr_field=Util.getField(transport.getClass(), "bind_addr");
            resolveAndAssignField(transport, bind_addr_field, cfg.getProperties(), ip_version);
            InetAddress resolved_addr=(InetAddress)Util.getField(bind_addr_field, transport);
            if(resolved_addr != null)
                ip_version=resolved_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;
            else if(ip_version == StackType.Dual)
                ip_version=StackType.IPv4; // prefer IPv4 addresses
        }

        for(int i=0; i < cfgs.size(); i++) {
            ProtocolConfiguration config=cfgs.get(i);
            Protocol prot=protocols.get(i);
            initializeAttrs(prot, config, ip_version);
        }
        setDefaultValues(cfgs, protocols, ip_version);
        ensureValidBindAddresses(protocols);
        return protocols;
    }



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
        }
        // basic protocol sanity check
        sanityCheck(protocol_list);
        return current_layer;
    }



    /**
     * Takes a list of configurations, creates a protocol for each and returns all protocols in a list.
     * @param protocol_configs A list of ProtocolConfigurations
     * @param stack            The protocol stack
     * @return List of Protocols
     */
    public static List<Protocol> createProtocols(List<ProtocolConfiguration> protocol_configs, ProtocolStack stack) throws Exception {
        List<Protocol> retval=new LinkedList<>();
        for(int i=0; i < protocol_configs.size(); i++) {
            ProtocolConfiguration protocol_config=protocol_configs.get(i);
            Protocol layer=createLayer(stack, protocol_config);
            if(layer == null)
                return null;
            retval.add(layer);
        }
        return retval;
    }


    protected static Protocol createLayer(ProtocolStack stack, ProtocolConfiguration config) throws Exception {
        String protocol_name=config.getProtocolName();
        if(protocol_name == null)
            return null;
        Class<? extends Protocol> clazz=Util.loadProtocolClass(protocol_name, stack != null? stack.getClass() : null);
        try {
            Protocol retval=clazz.getDeclaredConstructor().newInstance();
            if(stack != null)
                retval.setProtocolStack(stack);
            return retval;
        }
        catch(InstantiationException inst_ex) {
            throw new InstantiationException(String.format(Util.getMessage("ProtocolCreateError"), protocol_name, inst_ex.getLocalizedMessage()));
        }
    }

    /** Sets the attributes in a given protocol from properties */
    public static void initializeAttrs(Protocol prot, ProtocolConfiguration config, StackType ip_version) throws Exception {
        String protocol_name=config.getProtocolName();
        if(protocol_name == null)
            return;

        // before processing field and method based props, take dependencies specified by @Property.dependsUpon into account
        Map<String,String> properties=new HashMap<>(config.getProperties());
        AccessibleObject[] dependencyOrderedFieldsAndMethods=computePropertyDependencies(prot, properties);
        for(AccessibleObject ordered : dependencyOrderedFieldsAndMethods) {
            if(ordered instanceof Field)
                resolveAndAssignField(prot, (Field)ordered, properties, ip_version);
            else if(ordered instanceof Method)
                resolveAndInvokePropertyMethod(prot, (Method)ordered, properties, ip_version);
        }

        List<Object> additional_objects=prot.getConfigurableObjects();
        if(additional_objects != null && !additional_objects.isEmpty()) {
            for(Object obj : additional_objects) {
                resolveAndAssignFields(obj, properties, ip_version);
                resolveAndInvokePropertyMethods(obj, properties, ip_version);
            }
        }

        if(!properties.isEmpty())
            throw new IllegalArgumentException(String.format(Util.getMessage("ConfigurationError"), protocol_name, properties));

        // if we have protocol-specific XML configuration, pass it to the protocol
        List<Node> subtrees=config.getSubtrees();
        if(subtrees != null) {
            for(Node node : subtrees)
                prot.parse(node);
        }
    }


    /**
     * Throws an exception if sanity check fails. Possible sanity check is uniqueness of all protocol names
     */
    public static void sanityCheck(List<Protocol> protocols) throws Exception {
        Set<Short> ids=new HashSet<>();
        for(Protocol protocol : protocols) {
            short id=protocol.getId();
            String name=protocol.getName();
            if(id > 0 && !ids.add(id))
                throw new Exception("Protocol ID " + id + " (name=" + name + ") is duplicate; IDs have to be unique");
        }

        // For each protocol, get its required up and down services and check (if available) if they're satisfied
        for(Protocol protocol : protocols) {
            List<Integer> required_down_services=protocol.requiredDownServices();
            List<Integer> required_up_services=protocol.requiredUpServices();

            if(required_down_services != null && !required_down_services.isEmpty()) {
                // the protocols below 'protocol' have to provide the services listed in required_down_services
                List<Integer> tmp=new ArrayList<>(required_down_services);
                removeProvidedUpServices(protocol, tmp);
                if(!tmp.isEmpty())
                    throw new Exception("events " + printEvents(tmp) + " are required by " + protocol.getName() +
                                          ", but not provided by any of the protocols below it");
            }

            if(required_up_services != null && !required_up_services.isEmpty()) {
                // the protocols above 'protocol' have to provide the services listed in required_up_services
                List<Integer> tmp=new ArrayList<>(required_up_services);
                removeProvidedDownServices(protocol, tmp);
                if(!tmp.isEmpty())
                    throw new Exception("events " + printEvents(tmp) + " are required by " + protocol.getName() +
                                          ", but not provided by any of the protocols above it");
            }
        }
    }

    protected static String printEvents(List<Integer> events) {
        return events.stream().map(Event::type2String).collect(Collectors.joining(" ", "[", "]"));
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


    /** Returns all inet addresses found */
    public static Collection<InetAddress> getAddresses(Map<String,Map<String,InetAddressInfo>> map) throws Exception {
        return map.values().stream().flatMap(m -> m.values().stream())
          .flatMap(i -> i.getInetAddresses().stream()).filter(Objects::nonNull).collect(Collectors.toSet());
    }



    /*
     * Discovers all fields or methods within the protocol stack which set InetAddress, IpAddress, InetSocketAddress
     * (and lists of such) for which the user has specified a default value. Stores the resulting set of fields and
     * methods in a map of the form Protocol -> Property -> InetAddressInfo
     * where InetAddressInfo instances encapsulate the InetAddress related information of the fields and methods.
     */
    public static Map<String,Map<String,InetAddressInfo>> createInetAddressMap(List<ProtocolConfiguration> protocol_configs,
                                                                               List<Protocol> protocols) throws Exception {
        Map<String,Map<String,InetAddressInfo>> inetAddressMap=new HashMap<>();
        for(int i=0; i < protocol_configs.size(); i++) {
            ProtocolConfiguration protocol_config=protocol_configs.get(i);
            Protocol protocol=protocols.get(i);
            String protocolName=protocol.getName();

            // regenerate the properties which were destroyed during basic property processing
            Map<String,String> properties=new HashMap<>(protocol_config.getProperties());

            // check which InetAddress-related properties are non-null, and create an InetAddressInfo structure for them
            Method[] methods=Util.getAllDeclaredMethodsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < methods.length; j++) {
                if(isSetPropertyMethod(methods[j], protocol.getClass())) {
                    String propertyName=PropertyHelper.getPropertyName(methods[j]);
                    String propertyValue=properties.get(propertyName);

                    // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
                    String tmp=grabSystemProp(methods[j].getAnnotation(Property.class));
                    if(tmp != null)
                        propertyValue=tmp;

                    if(propertyValue != null && InetAddressInfo.isInetAddressRelated(methods[j])) {
                        Object converted=null;
                        try {
                            converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties, propertyValue,
                                                                       false, Util.getIpStackType());
                        }
                        catch(Exception e) {
                            throw new Exception("string could not be converted for method " + propertyName + " in "
                                                  + protocolName + " with default value " + propertyValue + ".Exception is " + e, e);
                        }
                        InetAddressInfo inetinfo=new InetAddressInfo(protocol, methods[j], properties, propertyValue, converted);
                        Map<String,InetAddressInfo> m=inetAddressMap.computeIfAbsent(protocolName, k -> new HashMap<>());
                        m.put(propertyName, inetinfo);
                    }
                }
            }

            // traverse class hierarchy and find all annotated fields and add them to the list if annotated
            Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < fields.length; j++) {
                String propertyName=PropertyHelper.getPropertyName(fields[j], properties);
                String propertyValue=properties.get(propertyName);

                // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
                String tmp=grabSystemProp(fields[j].getAnnotation(Property.class));
                if(tmp != null)
                    propertyValue=tmp;

                if((propertyValue != null || !PropertyHelper.usesDefaultConverter(fields[j]))
                  && InetAddressInfo.isInetAddressRelated(fields[j])) {
                    Object converted=null;
                    try {
                        converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties, propertyValue,
                                                                   false, Util.getIpStackType());
                    }
                    catch(Exception e) {
                        throw new Exception("string could not be converted for method " + propertyName + " in "
                                              + protocolName + " with default value " + propertyValue + ".Exception is " + e, e);
                    }
                    InetAddressInfo inetinfo=new InetAddressInfo(protocol, fields[j], properties, propertyValue, converted);
                    Map<String,InetAddressInfo> m=inetAddressMap.computeIfAbsent(protocolName, k -> new HashMap<>());
                    m.put(propertyName, inetinfo);
                }
            }
        }
        return inetAddressMap;
    }



    public static List<InetAddress> getInetAddresses(List<Protocol> protocols) throws Exception {
        List<InetAddress> retval=new LinkedList<>();

        // collect InetAddressInfo
        for(Protocol protocol : protocols) {
            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            for(Class<?> clazz=protocol.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
                Field[] fields=clazz.getDeclaredFields();
                for(int j=0; j < fields.length; j++) {
                    if(fields[j].isAnnotationPresent(Property.class)) {
                        if(InetAddressInfo.isInetAddressRelated(fields[j])) {
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


    /**
     * Method which processes @Property.defaultValue() values, associated with the annotation using the defaultValue()
     * annotation. This method does the following:
     * - find all properties which have no user value assigned
     * - if the defaultValue attribute is not "", generate a value for the field using the property converter for that
     *   property and assign it to the field
     */
    public static void setDefaultValues(List<ProtocolConfiguration> protocol_configs, List<Protocol> protocols,
                                        StackType ip_version) throws Exception {
        InetAddress default_ip_address=Util.getNonLoopbackAddress(ip_version);
        if(default_ip_address == null) {
            log.warn(Util.getMessage("OnlyLoopbackFound"), ip_version);
            default_ip_address=Util.getLoopback(ip_version);
        }

        for(int i=0; i < protocol_configs.size(); i++) {
            ProtocolConfiguration protocol_config=protocol_configs.get(i);
            Protocol protocol=protocols.get(i);
            String protocolName=protocol.getName();

            // regenerate the Properties which were destroyed during basic property processing
            Map<String,String> properties=new HashMap<>(protocol_config.getProperties());

            Method[] methods=Util.getAllDeclaredMethodsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < methods.length; j++) {
                if(isSetPropertyMethod(methods[j], protocol.getClass())) {
                    String propertyName=PropertyHelper.getPropertyName(methods[j]);
                    Object propertyValue=getValueFromProtocol(protocol, propertyName);
                    if(propertyValue == null) { // if propertyValue is null, check if there is a default value we can use
                        Property annotation=methods[j].getAnnotation(Property.class);

                        // get the default value for the method- check for InetAddress types
                        if(InetAddressInfo.isInetAddressRelated(methods[j])) {
                            String defaultValue=ip_version == StackType.IPv4? annotation.defaultValueIPv4() : annotation.defaultValueIPv6();
                            if(defaultValue != null && !defaultValue.isEmpty()) {
                                Object converted=null;
                                try {
                                    if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                        converted=default_ip_address;
                                    else
                                        converted=PropertyHelper.getConvertedValue(protocol, methods[j], properties,
                                                                                   defaultValue, true, ip_version);
                                    methods[j].invoke(protocol, converted);
                                }
                                catch(Exception e) {
                                    throw new Exception("default could not be assigned for method " + propertyName + " in "
                                                          + protocolName + " with default " + defaultValue, e);
                                }
                                log.debug("set property %s.%s to default value %s", protocolName, propertyName, converted);
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
                    if(InetAddressInfo.isInetAddressRelated(fields[j])) {
                        String defaultValue=ip_version == StackType.IPv4? annotation.defaultValueIPv4() : annotation.defaultValueIPv6();
                        if(defaultValue != null && !defaultValue.isEmpty()) {
                            // condition for invoking converter
                            if(defaultValue != null || !PropertyHelper.usesDefaultConverter(fields[j])) {
                                Object converted=null;
                                try {
                                    if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                        converted=default_ip_address;
                                    else
                                        converted=PropertyHelper.getConvertedValue(protocol, fields[j], properties,
                                                                                   defaultValue, true, ip_version);
                                    if(converted != null)
                                        Util.setField(fields[j], protocol, converted);
                                }
                                catch(Exception e) {
                                    throw new Exception("default could not be assigned for field " + propertyName + " in "
                                                          + protocolName + " with default value " + defaultValue, e);
                                }
                                log.debug("set property " + protocolName + "." + propertyName + " to default value " + converted);
                            }
                        }
                    }
                }
            }
        }
    }


    public static void setDefaultValues(List<Protocol> protocols, StackType ip_version) throws Exception {
        InetAddress default_ip_address=Util.getNonLoopbackAddress(ip_version);
        if(default_ip_address == null) {
            log.warn(Util.getMessage("OnlyLoopbackFound"), ip_version);
            default_ip_address=Util.getLoopback(ip_version);
        }

        for(Protocol protocol : protocols) {
            String protocolName=protocol.getName();

            //traverse class hierarchy and find all annotated fields and add them to the list if annotated
            Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(protocol.getClass(), Property.class);
            for(int j=0; j < fields.length; j++) {
                // get the default value for the field - check for InetAddress types
                if(InetAddressInfo.isInetAddressRelated(fields[j])) {
                    Object propertyValue=getValueFromProtocol(protocol, fields[j]);
                    if(propertyValue == null) {
                        // add to collection of @Properties with no user specified value
                        Property annotation=fields[j].getAnnotation(Property.class);

                        String defaultValue=ip_version == StackType.IPv6? annotation.defaultValueIPv6() : annotation.defaultValueIPv4();
                        if(defaultValue != null && !defaultValue.isEmpty()) {
                            // condition for invoking converter
                            Object converted=null;
                            try {
                                if(defaultValue.equalsIgnoreCase(Global.NON_LOOPBACK_ADDRESS))
                                    converted=default_ip_address;
                                else
                                    converted=PropertyHelper.getConvertedValue(protocol, fields[j], defaultValue, true, ip_version);
                                if(converted != null)
                                    Util.setField(fields[j], protocol, converted);
                            }
                            catch(Exception e) {
                                throw new Exception("default could not be assigned for field " + fields[j].getName() + " in "
                                                      + protocolName + " with default value " + defaultValue, e);
                            }
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


    public static <T extends Object> T getValueFromProtocol(Protocol protocol, Field field) throws IllegalAccessException {
        if(protocol == null || field == null) return null;
        if(!Modifier.isPublic(field.getModifiers()))
            field.setAccessible(true);
        return (T)field.get(protocol);
    }


    public static <T extends Object> T getValueFromProtocol(Protocol protocol, String field_name) throws IllegalAccessException {
        if(protocol == null || field_name == null) return null;
        Field field=Util.getField(protocol.getClass(), field_name);
        return field != null? getValueFromProtocol(protocol, field) : null;
    }


    /**
     * This method creates a list of all properties (field or method) in dependency order,
     * where dependencies are specified using the dependsUpon specifier of the Property annotation.
     * In particular, it does the following:
     * (i) creates a master list of properties
     * (ii) checks that all dependency references are present
     * (iii) creates a copy of the master list in dependency order
     */
    static AccessibleObject[] computePropertyDependencies(Object obj, Map<String,String> properties) {

        // List of Fields and Methods of the protocol annotated with @Property
        List<AccessibleObject> unorderedFieldsAndMethods=new LinkedList<>();
        List<AccessibleObject> orderedFieldsAndMethods=new LinkedList<>();
        // Maps property name to property object
        Map<String,AccessibleObject> propertiesInventory=new HashMap<>();

        // get the methods for this class and add them to the list if annotated with @Property
        Method[] methods=obj.getClass().getMethods();
        for(int i=0; i < methods.length; i++) {
            if(methods[i].isAnnotationPresent(Property.class) && isSetPropertyMethod(methods[i], obj.getClass())) {
                String propertyName=PropertyHelper.getPropertyName(methods[i]);
                unorderedFieldsAndMethods.add(methods[i]);
                propertiesInventory.put(propertyName, methods[i]);
            }
        }
        //traverse class hierarchy and find all annotated fields and add them to the list if annotated
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(int i=0; i < fields.length; i++) {
                if(fields[i].isAnnotationPresent(Property.class)) {
                    String propertyName=PropertyHelper.getPropertyName(fields[i], properties);
                    unorderedFieldsAndMethods.add(fields[i]);
                    // may need to change this based on name parameter of Property
                    propertiesInventory.put(propertyName, fields[i]);
                }
            }
        }

        // at this stage, we have all Fields and Methods annotated with @Property
        checkDependencyReferencesPresent(unorderedFieldsAndMethods, propertiesInventory);

        // order the fields and methods by dependency
        orderedFieldsAndMethods=orderFieldsAndMethodsByDependency(unorderedFieldsAndMethods, propertiesInventory);

        // convert to array of Objects
        AccessibleObject[] result=new AccessibleObject[orderedFieldsAndMethods.size()];
        for(int i=0; i < orderedFieldsAndMethods.size(); i++)
            result[i]=orderedFieldsAndMethods.get(i);
        return result;
    }

    static List<AccessibleObject> orderFieldsAndMethodsByDependency(List<AccessibleObject> unorderedList,
                                                                    Map<String,AccessibleObject> propertiesMap) {
        // Stack to detect cycle in depends relation
        Stack<AccessibleObject> stack=new Stack<>();
        // the result list
        List<AccessibleObject> orderedList=new LinkedList<>();

        // add the elements from the unordered list to the ordered list
        // any dependencies will be checked and added first, in recursive manner
        for(int i=0; i < unorderedList.size(); i++) {
            AccessibleObject obj=unorderedList.get(i);
            addPropertyToDependencyList(orderedList, propertiesMap, stack, obj);
        }

        return orderedList;
    }

    /**
     * DFS of dependency graph formed by Property annotations and dependsUpon parameter
     * This is used to create a list of Properties in dependency order
     */
    static void addPropertyToDependencyList(List<AccessibleObject> orderedList, Map<String,AccessibleObject> props, Stack<AccessibleObject> stack, AccessibleObject obj) {
        if(orderedList.contains(obj))
            return;
        if(stack.search(obj) > 0)
            throw new RuntimeException("Deadlock in @Property dependency processing");
        // record the fact that we are processing obj
        stack.push(obj);
        // process dependencies for this object before adding it to the list
        Property annotation=obj.getAnnotation(Property.class);
        String dependsClause=annotation.dependsUpon();
        StringTokenizer st=new StringTokenizer(dependsClause, ",");
        while(st.hasMoreTokens()) {
            String token=st.nextToken().trim();
            AccessibleObject dep=props.get(token);
            // if null, throw exception
            addPropertyToDependencyList(orderedList, props, stack, dep);
        }
        // indicate we're done with processing dependencies
        stack.pop();
        // we can now add in dependency order
        orderedList.add(obj);
    }

    /*
     * Checks that for every dependency referred, there is a matching property
     */
    static void checkDependencyReferencesPresent(List<AccessibleObject> objects, Map<String,AccessibleObject> props) {
        for(int i=0; i < objects.size(); i++) { // iterate overall properties marked by @Property
            // get the Property annotation
            AccessibleObject ao=objects.get(i);
            Property annotation=ao.getAnnotation(Property.class);
            if(annotation == null) {
                throw new IllegalArgumentException("@Property annotation is required for checking dependencies;" +
                                                     " annotation is missing for Field/Method " + ao.toString());
            }

            String dependsClause=annotation.dependsUpon();
            if(dependsClause.trim().isEmpty())
                continue;

            // split dependsUpon specifier into tokens; trim each token; search for token in list
            StringTokenizer st=new StringTokenizer(dependsClause, ",");
            while(st.hasMoreTokens()) {
                String token=st.nextToken().trim();

                // check that the string representing a property name is in the list
                boolean found=false;
                Set<String> keyset=props.keySet();
                for(Iterator<String> iter=keyset.iterator(); iter.hasNext(); ) {
                    if(iter.next().equals(token)) {
                        found=true;
                        break;
                    }
                }
                if(!found)
                    throw new IllegalArgumentException("@Property annotation " + annotation.name() +
                                                         " has an unresolved dependsUpon property: " + token);
            }
        }

    }

    public static void resolveAndInvokePropertyMethods(Object obj, Map<String,String> props, StackType ip_version) throws Exception {
        Method[] methods=obj.getClass().getMethods();
        for(Method method : methods) {
            resolveAndInvokePropertyMethod(obj, method, props, ip_version);
        }
    }

    public static void resolveAndInvokePropertyMethod(Object obj, Method method, Map<String,String> props,
                                                      StackType ip_version) throws Exception {
        String methodName=method.getName();
        Property annotation=method.getAnnotation(Property.class);
        if(annotation != null && isSetPropertyMethod(method, obj.getClass())) {
            String propertyName=PropertyHelper.getPropertyName(method);
            String propertyValue=props.get(propertyName);

            // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
            String tmp=grabSystemProp(method.getAnnotation(Property.class));
            if(tmp != null)
                propertyValue=tmp;

            if(propertyName != null && propertyValue != null) {
                String deprecated_msg=annotation.deprecatedMessage();
                if(deprecated_msg != null && !deprecated_msg.isEmpty()) {
                    log.warn(Util.getMessage("Deprecated"), method.getDeclaringClass().getSimpleName() + "." + methodName,
                             deprecated_msg);
                }
            }

            if(propertyValue != null) {
                Object converted=null;
                try {
                    converted=PropertyHelper.getConvertedValue(obj, method, props, propertyValue, true, ip_version);
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

    public static void resolveAndAssignFields(Object obj, Map<String,String> props, StackType ip_version) throws Exception {
        //traverse class hierarchy and find all annotated fields
        for(Class<?> clazz=obj.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(Field field : fields)
                resolveAndAssignField(obj, field, props, ip_version);
        }
    }

    public static void resolveAndAssignField(Object obj, Field field, Map<String,String> props, StackType ip_version) throws Exception {
        Property annotation=field.getAnnotation(Property.class);
        if(annotation != null) {
            String propertyName=PropertyHelper.getPropertyName(field, props);
            String propertyValue=props.get(propertyName);

            // if there is a systemProperty attribute defined in the annotation, set the property value from the system property
            // only do this if the property value hasn't yet been set
            if(propertyValue == null) {
                String tmp=grabSystemProp(field.getAnnotation(Property.class));
                if(tmp != null)
                    propertyValue=tmp;
            }

            if(propertyName != null && propertyValue != null) {
                String deprecated_msg=annotation.deprecatedMessage();
                if(deprecated_msg != null && !deprecated_msg.isEmpty()) {
                    log.warn(Util.getMessage("Deprecated"), field.getDeclaringClass().getSimpleName() + "." + field.getName(),
                             deprecated_msg);
                }
            }

            if(propertyValue != null || !PropertyHelper.usesDefaultConverter(field)) {
                Object converted=null;
                try {
                    converted=PropertyHelper.getConvertedValue(obj, field, props, propertyValue, true, ip_version);
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


    public static boolean isSetPropertyMethod(Method method) {
        return (method.getName().startsWith("set")
          && method.getReturnType() == java.lang.Void.TYPE
          && method.getParameterTypes().length == 1);
    }

    public static boolean isSetPropertyMethod(Method method, Class<?> enclosing_clazz) {
        return (method.getName().startsWith("set")
          && (method.getReturnType() == java.lang.Void.TYPE || enclosing_clazz.isAssignableFrom(method.getReturnType()))
          && method.getParameterTypes().length == 1);
    }


    private static String grabSystemProp(Property annotation) {
        String[] system_property_names=annotation.systemProperty();
        String retval=null;

        for(String system_property_name : system_property_names) {
            if(system_property_name != null && !system_property_name.isEmpty()) {
                try {
                    retval=System.getProperty(system_property_name);
                    if(retval != null)
                        return retval;
                }
                catch(SecurityException ex) {
                    log.error(Util.getMessage("SyspropFailure"), system_property_name, ex);
                }

                try {
                    retval=System.getenv(system_property_name);
                    if(retval != null)
                        return retval;
                }
                catch(SecurityException ex) {
                    log.error(Util.getMessage("SyspropFailure"), system_property_name, ex);
                }

            }
        }
        return retval;
    }

    /* --------------------------- End of Private Methods ---------------------------------- */


    public static class InetAddressInfo {
        Protocol protocol;
        AccessibleObject fieldOrMethod;
        Map<String,String> properties;
        String propertyName;
        String stringValue;
        Object convertedValue;
        boolean isField;
        boolean isParameterized; // is the associated type parametrized? (e.g. Collection<String>)
        Object baseType;         // what is the base type (e.g. Collection)

        InetAddressInfo(Protocol protocol, AccessibleObject fieldOrMethod, Map<String,String> properties, String stringValue,
                        Object convertedValue) {
            // check input values
            if(protocol == null) {
                throw new IllegalArgumentException("Protocol for Field/Method must be non-null");
            }
            if(fieldOrMethod instanceof Field) {
                isField=true;
            }
            else if(fieldOrMethod instanceof Method) {
                isField=false;
            }
            else
                throw new IllegalArgumentException("AccesibleObject is neither Field nor Method");
            if(properties == null) {
                throw new IllegalArgumentException("Properties for Field/Method must be non-null");
            }

            // set the values passed by the user - need to check for null
            this.protocol=protocol;
            this.fieldOrMethod=fieldOrMethod;
            this.properties=properties;
            this.stringValue=stringValue;
            this.convertedValue=convertedValue;

            // set the property name
            if(isField())
                propertyName=PropertyHelper.getPropertyName((Field)fieldOrMethod, properties);
            else
                propertyName=PropertyHelper.getPropertyName((Method)fieldOrMethod);

            // is variable type parameterized
            this.isParameterized=false;
            if(isField())
                this.isParameterized=hasParameterizedType((Field)fieldOrMethod);
            else
                this.isParameterized=hasParameterizedType((Method)fieldOrMethod);

            // if parameterized, what is the base type?
            this.baseType=null;
            if(isField() && isParameterized) {
                // the Field has a single type
                ParameterizedType fpt=(ParameterizedType)((Field)fieldOrMethod).getGenericType();
                this.baseType=fpt.getActualTypeArguments()[0];
            }
            else if(!isField() && isParameterized) {
                // the Method has several parameters (and so types)
                Type[] types=((Method)fieldOrMethod).getGenericParameterTypes();
                ParameterizedType mpt=(ParameterizedType)types[0];
                this.baseType=mpt.getActualTypeArguments()[0];
            }
        }

        boolean isField() {
            return isField;
        }

        String getStringValue() {
            return stringValue;
        }

        String getPropertyName() {
            return propertyName;
        }

        Object getConvertedValue() {
            return convertedValue;
        }

        boolean isParameterized() {
            return isParameterized;
        }

        Object getBaseType() {
            return baseType;
        }

        static boolean hasParameterizedType(Field f) {
            if(f == null) {
                throw new IllegalArgumentException("Field argument is null");
            }
            Type type=f.getGenericType();
            return (type instanceof ParameterizedType);
        }

        static boolean hasParameterizedType(Method m) throws IllegalArgumentException {
            if(m == null) {
                throw new IllegalArgumentException("Method argument is null");
            }
            Type[] types=m.getGenericParameterTypes();
            return (types[0] instanceof ParameterizedType);
        }

        static boolean isInetAddressRelated(Field f) {
            if(hasParameterizedType(f)) {
                // check for List<InetAddress>, List<InetSocketAddress>, List<IpAddress>
                ParameterizedType fieldtype=(ParameterizedType)f.getGenericType();
                // check that this parameterized type satisfies our constraints
                try {
                    parameterizedTypeSanityCheck(fieldtype);
                }
                catch(IllegalArgumentException e) {
                    // because this Method's parameter fails the sanity check, its probably not an InetAddress related structure
                    return false;
                }

                Class<?> listType=(Class<?>)fieldtype.getActualTypeArguments()[0];
                return isInetAddressOrCompatibleType(listType);
            }
            else {
                // check if the non-parameterized type is InetAddress, InetSocketAddress or IpAddress
                Class<?> fieldtype=f.getType();
                return isInetAddressOrCompatibleType(fieldtype);
            }
        }

        /*
         * Checks if this method's single parameter represents of of the following:
         * an InetAddress, IpAddress or InetSocketAddress or one of
         * List<InetAddress>, List<IpAddress> or List<InetSocketAddress>
         */
        static boolean isInetAddressRelated(Method m) {
            if(hasParameterizedType(m)) {
                Type[] types=m.getGenericParameterTypes();
                ParameterizedType methodParamType=(ParameterizedType)types[0];
                // check that this parameterized type satisfies our constraints
                try {
                    parameterizedTypeSanityCheck(methodParamType);
                }
                catch(IllegalArgumentException e) {
                    // because this Method's parameter fails the sanity check, its probably not an InetAddress
                    return false;
                }

                Class<?> listType=(Class<?>)methodParamType.getActualTypeArguments()[0];
                return isInetAddressOrCompatibleType(listType);
            }
            else {
                Class<?> methodParamType=m.getParameterTypes()[0];
                return isInetAddressOrCompatibleType(methodParamType);
            }
        }

        static boolean isInetAddressOrCompatibleType(Class<?> c) {
            return InetAddress.class.isAssignableFrom(c)
              || SocketAddress.class.isAssignableFrom(c)
              || PhysicalAddress.class.isAssignableFrom(c);
        }

        /*
         * Check if the parameterized type represents one of:
         * List<InetAddress>, List<IpAddress>, List<InetSocketAddress>
         */
        static void parameterizedTypeSanityCheck(ParameterizedType pt) throws IllegalArgumentException {

            Type rawType=pt.getRawType();
            Type[] actualTypes=pt.getActualTypeArguments();

            // constraints on use of parameterized types with @Property
            if(!(rawType instanceof Class<?> && Collection.class.isAssignableFrom((Class<?>)rawType))) {
                throw new IllegalArgumentException("Invalid parameterized type definition - parameterized type must be a collection");
            }
            // check for non-parameterized type in List
            if(!(actualTypes[0] instanceof Class<?>)) {
                throw new IllegalArgumentException("Invalid parameterized type - List must not contain a parameterized type");
            }
        }

        /*
         * Converts the computedValue to a list of InetAddresses.
         * Because computedValues may be null, we need to return
         * a zero length list in some cases.
         */
        public List<InetAddress> getInetAddresses() {
            List<InetAddress> addresses=new ArrayList<>();
            if(getConvertedValue() == null)
                return addresses;
            // if we take only an InetAddress argument
            if(!isParameterized()) {
                addresses.add(getInetAddress(getConvertedValue()));
                return addresses;
            }
            // if we take a List<InetAddress> or similar
            else {
                List<?> values=(List<?>)getConvertedValue();
                if(values.isEmpty())
                    return addresses;
                for(int i=0; i < values.size(); i++) {
                    addresses.add(getInetAddress(values.get(i)));
                }
                return addresses;
            }
        }

        private static InetAddress getInetAddress(Object obj) throws IllegalArgumentException {
            if(obj == null)
                throw new IllegalArgumentException("Input argument must represent a non-null IP address");
            if(obj instanceof InetAddress)
                return (InetAddress)obj;
            else if(obj instanceof IpAddress)
                return ((IpAddress)obj).getIpAddress();
            else if(obj instanceof InetSocketAddress)
                return ((InetSocketAddress)obj).getAddress();
            else
                throw new IllegalArgumentException("Input argument does not represent one of InetAddress. IpAddress not InetSocketAddress");
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("InetAddressInfo(")
              .append("protocol=" + protocol.getName())
              .append(", propertyName=" + getPropertyName())
              .append(", string value=" + getStringValue())
              .append(", parameterized=" + isParameterized());
            if(isParameterized())
                sb.append(", baseType=" + getBaseType());
            sb.append(")");
            return sb.toString();
        }
    }


}
