package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.TpHeader;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
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
 * @version $Id: Configurator.java,v 1.30 2007/12/03 14:33:58 belaban Exp $
 */
public class Configurator {

     protected static final Log log=LogFactory.getLog(Configurator.class);


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
    public static Protocol setupProtocolStack(String configuration, ProtocolStack st) throws Exception {
        Protocol protocol_stack=null;
        Vector<ProtocolConfiguration> protocol_configs;
        Vector<Protocol> protocols;

        protocol_configs=parseConfigurations(configuration);
        protocols=createProtocols(protocol_configs, st);
        if(protocols == null)
            return null;
        protocol_stack=connectProtocols(protocols);
        return protocol_stack;
    }


    public static void initProtocolStack(List<Protocol> protocols) throws Exception {
        Collections.reverse(protocols);
        for(Protocol prot: protocols) {
            prot.init();
        }
    }

    public static void startProtocolStack(List<Protocol> protocols, String cluster_name, final Map<String,Tuple<TP,Short>> singletons) throws Exception {
        Protocol above_prot=null;
        for(final Protocol prot: protocols) {
            String singleton_name=Util.getProperty(prot, Global.SINGLETON_NAME);
            if(singleton_name != null && singleton_name.length() > 0) {
                TP transport=(TP)prot;
                final Map<String, Protocol> up_prots=transport.getUpProtocols();
                synchronized(up_prots) {
                    Set<String> keys=up_prots.keySet();
                    if(keys.contains(cluster_name))
                        throw new IllegalStateException("cluster '" + cluster_name + "' is already connected to singleton " +
                                "transport: " + keys);

                    for(Iterator<Map.Entry<String,Protocol>> it=up_prots.entrySet().iterator(); it.hasNext();) {
                        Map.Entry<String,Protocol> entry=it.next();
                        Protocol tmp=entry.getValue();
                        if(tmp == above_prot) {
                            it.remove();
                        }
                    }

                    if(above_prot != null) {
                        ProtocolAdapter ad=new ProtocolAdapter(cluster_name, prot.getName(), above_prot, prot);
                        above_prot.setDownProtocol(ad);
                        up_prots.put(cluster_name, ad);
                    }
                }
                synchronized(singletons) {
                    Tuple<TP,Short> val=singletons.get(singleton_name);
                    if(val == null) {
                        singletons.put(singleton_name, new Tuple<TP,Short>(transport,(short)1));
                    }
                    else {
                        short num_starts=val.getVal2();
                        val.setVal2((short)(num_starts +1));
                        if(num_starts >= 1) {
                            if(above_prot != null)
                                above_prot.up(new Event(Event.SET_LOCAL_ADDRESS, transport.getLocalAddress()));
                            continue;
                        }
                    }
                }
            }
            prot.start();
            above_prot=prot;
        }
    }

    public static void stopProtocolStack(List<Protocol> protocols, final Map<String,Tuple<TP,Short>> singletons) {
        for(final Protocol prot: protocols) {
            String singleton_name=Util.getProperty(prot, Global.SINGLETON_NAME);
            if(singleton_name != null && singleton_name.length() > 0) {
                synchronized(singletons) {
                    Tuple<TP,Short> val=singletons.get(singleton_name);
                    if(val != null) {
                        short num_starts=(short)Math.max(val.getVal2() -1, 0);
                        val.setVal2(num_starts);
                        if(num_starts > 0) {
                            continue; // don't call TP.stop() if we still have references to the transport
                        }
                        else
                            singletons.remove(singleton_name);
                    }
                }
            }
            prot.stop();
        }
    }


    public static void destroyProtocolStack(List<Protocol> protocols) {
        for(Protocol prot: protocols) {
            prot.destroy();
        }
    }


    public static Protocol findProtocol(Protocol prot_stack, String name) {
        String s;
        Protocol curr_prot=prot_stack;

        while(true) {
            s=curr_prot.getName();
            if(s == null)
                continue;
            if(s.equals(name))
                return curr_prot;
            curr_prot=curr_prot.getDownProtocol();
            if(curr_prot == null)
                break;
        }
        return null;
    }


    public static Protocol getBottommostProtocol(Protocol prot_stack) {
        Protocol tmp=null, curr_prot=prot_stack;

        while(true) {
            if((tmp=curr_prot.getDownProtocol()) == null)
                break;
            curr_prot=tmp;
        }
        return curr_prot;
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


    /**
     * Inserts an already created (and initialized) protocol into the protocol list. Sets the links
     * to the protocols above and below correctly and adjusts the linked list of protocols accordingly.
     * This should be done before starting the stack.
     * @param prot  The protocol to be inserted. Before insertion, a sanity check will ensure that none
     *              of the existing protocols have the same name as the new protocol.
     * @param position Where to place the protocol with respect to the neighbor_prot (ABOVE, BELOW)
     * @param neighbor_prot The name of the neighbor protocol. An exception will be thrown if this name
     *                      is not found
     * @param stack The protocol stack
     * @exception Exception Will be thrown when the new protocol cannot be created, or inserted.
     */
    public static void insertProtocol(Protocol prot, int position, String neighbor_prot, ProtocolStack stack) throws Exception {
        if(neighbor_prot == null) throw new Exception("Configurator.insertProtocol(): neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new Exception("position has to be ABOVE or BELOW");


        Protocol neighbor=stack.findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new Exception("protocol \"" + neighbor_prot + "\" not found in " + stack.printProtocolSpec(false));

         // connect to the protocol layer below and above
        if(position == ProtocolStack.BELOW) {
            prot.setUpProtocol(neighbor);
            Protocol below=neighbor.getDownProtocol();
            prot.setDownProtocol(below);
            if(below != null)
                below.setUpProtocol(prot);
            neighbor.setDownProtocol(prot);
        }
        else { // ABOVE is default
            Protocol above=neighbor.getUpProtocol();
            prot.setUpProtocol(above);
            if(above != null)
                above.setDownProtocol(prot);
            prot.setDownProtocol(neighbor);
            neighbor.setUpProtocol(prot);
        }
    }


    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public static Protocol removeProtocol(Protocol top_prot, String prot_name) throws Exception {
        if(prot_name == null) return null;
        Protocol prot=findProtocol(top_prot, prot_name);
        if(prot == null) return null;
        Protocol above=prot.getUpProtocol(), below=prot.getDownProtocol();
        if(above != null)
            above.setDownProtocol(below);
        if(below != null)
            below.setUpProtocol(above);
        prot.setUpProtocol(null);
        prot.setDownProtocol(null);
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
    private static Protocol connectProtocols(Vector protocol_list) {
        Protocol current_layer=null, next_layer=null;

        for(int i=0; i < protocol_list.size(); i++) {
            current_layer=(Protocol)protocol_list.elementAt(i);
            if(i + 1 >= protocol_list.size())
                break;
            next_layer=(Protocol)protocol_list.elementAt(i + 1);
            next_layer.setDownProtocol(current_layer);
            current_layer.setUpProtocol(next_layer);

             if(current_layer instanceof TP) {
                String singleton_name=Util.getProperty(current_layer, Global.SINGLETON_NAME);
                if(singleton_name != null && singleton_name.length() > 0) {
                    ConcurrentMap<String, Protocol> up_prots=((TP)current_layer).getUpProtocols();
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
    public static Vector<String> parseProtocols(String config_str) throws IOException {
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
    public static Vector<ProtocolConfiguration> parseConfigurations(String configuration) throws Exception {
        Vector<ProtocolConfiguration> retval=new Vector<ProtocolConfiguration>();
        Vector protocol_string=parseProtocols(configuration);
        String component_string;
        ProtocolConfiguration protocol_config;

        if(protocol_string == null)
            return null;
        for(int i=0; i < protocol_string.size(); i++) {
            component_string=(String)protocol_string.elementAt(i);
            protocol_config=new ProtocolConfiguration(component_string);
            retval.addElement(protocol_config);
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
    private static Vector<Protocol> createProtocols(Vector<ProtocolConfiguration> protocol_configs, final ProtocolStack stack) throws Exception {
        Vector<Protocol> retval=new Vector<Protocol>();
        ProtocolConfiguration protocol_config;
        Protocol layer;
        String singleton_name;

        for(int i=0; i < protocol_configs.size(); i++) {
            protocol_config=protocol_configs.elementAt(i);
            singleton_name=protocol_config.getProperties().getProperty(Global.SINGLETON_NAME);
            if(singleton_name != null && singleton_name.trim().length() > 0) {
                synchronized(stack) {
                    if(i > 0) { // crude way to check whether protocol is a transport
                        throw new IllegalArgumentException("Property 'singleton_name' can only be used in a transport" +
                                " protocol (was used in " + protocol_config.getProtocolName() + ")");
                    }
                    Map<String,Tuple<TP,Short>> singleton_transports=ProtocolStack.getSingletonTransports();
                    Tuple<TP,Short> val=singleton_transports.get(singleton_name);
                    layer=val != null? val.getVal1() : null;
                    if(layer != null) {
                        retval.add(layer);
                    }
                    else {
                        layer=protocol_config.createLayer(stack);
                        if(layer == null)
                            return null;
                        singleton_transports.put(singleton_name, new Tuple<TP,Short>((TP)layer,(short)0));
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
        sanityCheck(retval);
        return retval;
    }


    /**
     Throws an exception if sanity check fails. Possible sanity check is uniqueness of all protocol names
     */
    public static void sanityCheck(Vector<Protocol> protocols) throws Exception {
        Vector<String> names=new Vector<String>();
        Protocol prot;
        String name;
        ProtocolReq req;
        Vector<ProtocolReq> req_list=new Vector<ProtocolReq>();
        int evt_type;

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
        for(int i=0; i < protocols.size(); i++) {
            prot=protocols.elementAt(i);
            req=new ProtocolReq(prot.getName());
            req.up_reqs=prot.requiredUpServices();
            req.down_reqs=prot.requiredDownServices();
            req.up_provides=prot.providedUpServices();
            req.down_provides=prot.providedDownServices();
            req_list.addElement(req);
        }


        for(int i=0; i < req_list.size(); i++) {
            req=req_list.elementAt(i);

            // check whether layers above this one provide corresponding down services
            if(req.up_reqs != null) {
                for(int j=0; j < req.up_reqs.size(); j++) {
                    evt_type=((Integer)req.up_reqs.elementAt(j)).intValue();

                    if(!providesDownServices(i, req_list, evt_type)) {
                        throw new Exception("Configurator.sanityCheck(): event " +
                                            Event.type2String(evt_type) + " is required by " +
                                            req.name + ", but not provided by any of the layers above");
                    }
                }
            }

            // check whether layers below this one provide corresponding up services
            if(req.down_reqs != null) {  // check whether layers above this one provide up_reqs
                for(int j=0; j < req.down_reqs.size(); j++) {
                    evt_type=((Integer)req.down_reqs.elementAt(j)).intValue();

                    if(!providesUpServices(i, req_list, evt_type)) {
                        throw new Exception("Configurator.sanityCheck(): event " +
                                            Event.type2String(evt_type) + " is required by " +
                                            req.name + ", but not provided by any of the layers below");
                    }
                }
            }

        }
    }


    /** Check whether any of the protocols 'below' end_index provide evt_type */
    static boolean providesUpServices(int end_index, Vector req_list, int evt_type) {
        ProtocolReq req;

        for(int i=0; i < end_index; i++) {
            req=(ProtocolReq)req_list.elementAt(i);
            if(req.providesUpService(evt_type))
                return true;
        }
        return false;
    }


    /** Checks whether any of the protocols 'above' start_index provide evt_type */
    static boolean providesDownServices(int start_index, Vector req_list, int evt_type) {
        ProtocolReq req;

        for(int i=start_index; i < req_list.size(); i++) {
            req=(ProtocolReq)req_list.elementAt(i);
            if(req.providesDownService(evt_type))
                return true;
        }
        return false;
    }



    /* --------------------------- End of Private Methods ---------------------------------- */





    private static class ProtocolReq {
        Vector up_reqs=null;
        Vector down_reqs=null;
        Vector up_provides=null;
        Vector down_provides=null;
        String name=null;

        ProtocolReq(String name) {
            this.name=name;
        }


        boolean providesUpService(int evt_type) {
            int type;

            if(up_provides != null) {
                for(int i=0; i < up_provides.size(); i++) {
                    type=((Integer)up_provides.elementAt(i)).intValue();
                    if(type == evt_type)
                        return true;
                }
            }
            return false;
        }

        boolean providesDownService(int evt_type) {
            int type;

            if(down_provides != null) {
                for(int i=0; i < down_provides.size(); i++) {
                    type=((Integer)down_provides.elementAt(i)).intValue();
                    if(type == evt_type)
                        return true;
                }
            }
            return false;
        }


        public String toString() {
            StringBuilder ret=new StringBuilder();
            ret.append('\n' + name + ':');
            if(up_reqs != null)
                ret.append("\nRequires from above: " + printUpReqs());

            if(down_reqs != null)
                ret.append("\nRequires from below: " + printDownReqs());

            if(up_provides != null)
                ret.append("\nProvides to above: " + printUpProvides());

            if(down_provides != null)
                ret.append("\nProvides to below: ").append(printDownProvides());
            return ret.toString();
        }


        String printUpReqs() {
            StringBuffer ret;
            ret=new StringBuffer("[");
            if(up_reqs != null) {
                for(int i=0; i < up_reqs.size(); i++) {
                    ret.append(Event.type2String(((Integer)up_reqs.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }

        String printDownReqs() {
            StringBuilder ret=new StringBuilder("[");
            if(down_reqs != null) {
                for(int i=0; i < down_reqs.size(); i++) {
                    ret.append(Event.type2String(((Integer)down_reqs.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }


        String printUpProvides() {
            StringBuilder ret=new StringBuilder("[");
            if(up_provides != null) {
                for(int i=0; i < up_provides.size(); i++) {
                    ret.append(Event.type2String(((Integer)up_provides.elementAt(i)).intValue()) + ' ');
                }
            }
            return ret.toString() + ']';
        }

        String printDownProvides() {
            StringBuilder ret=new StringBuilder("[");
            if(down_provides != null) {
                for(int i=0; i < down_provides.size(); i++)
                    ret.append(Event.type2String(((Integer)down_provides.elementAt(i)).intValue()) +
                               ' ');
            }
            return ret.toString() + ']';
        }

    }


    /**
     * Parses and encapsulates the specification for 1 protocol of the protocol stack, e.g.
     * <code>UNICAST(timeout=5000)</code>
     */
    public static class ProtocolConfiguration {
        private String protocol_name=null;
        private String properties_str=null;
        private final Properties properties=new Properties();
        private static final String protocol_prefix="org.jgroups.protocols";


        /**
         * Creates a new ProtocolConfiguration.
         * @param config_str The configuration specification for the protocol, e.g.
         *                   <pre>VERIFY_SUSPECT(timeout=1500)</pre>
         */
        public ProtocolConfiguration(String config_str) throws Exception {
            setContents(config_str);
        }

        public ProtocolConfiguration() {
        }

        public String getProtocolName() {
            return protocol_name;
        }

        public void setProtocolName(String name) {
            protocol_name=name;
        }

        public Properties getProperties() {
            return properties;
        }

        public void setPropertiesString(String props) {
            this.properties_str=props;
        }

        void setContents(String config_str) throws Exception {
            int index=config_str.indexOf('(');  // e.g. "UDP(in_port=3333)"
            int end_index=config_str.lastIndexOf(')');

            if(index == -1) {
                protocol_name=config_str;
            }
            else {
                if(end_index == -1) {
                    throw new Exception("Configurator.ProtocolConfiguration.setContents(): closing ')' " +
                                        "not found in " + config_str + ": properties cannot be set !");
                }
                else {
                    properties_str=config_str.substring(index + 1, end_index);
                    protocol_name=config_str.substring(0, index);
                }
            }

            /* "in_port=5555;out_port=6666" */
            if(properties_str != null) {
                String[] components=properties_str.split(";");
                for(int i=0; i < components.length; i++) {
                    String name, value, comp=components[i];
                    index=comp.indexOf('=');
                    if(index == -1) {
                        throw new Exception("Configurator.ProtocolConfiguration.setContents(): '=' not found in " + comp);
                    }
                    name=comp.substring(0, index);
                    value=comp.substring(index + 1, comp.length());
                    properties.put(name, value);
                }
            }
        }


        private Protocol createLayer(ProtocolStack prot_stack) throws Exception {
            Protocol retval=null;
            if(protocol_name == null)
                return null;

            String defaultProtocolName=protocol_prefix + '.' + protocol_name;
            Class clazz=null;

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
                if(properties != null)
                    if(!retval.setPropertiesInternal(properties))
                        throw new IllegalArgumentException("the following properties in " + protocol_name +
                                " are not recognized: " + properties);
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


    private static class ProtocolAdapter extends Protocol {
        final String cluster_name;
        final String transport_name;
        final TpHeader header;

        private ProtocolAdapter(String cluster_name, String transport_name, Protocol up, Protocol down) {
            this.cluster_name=cluster_name;
            this.transport_name=transport_name;
            this.up_prot=up;
            this.down_prot=down;
            this.header=new TpHeader(cluster_name);
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                msg.putHeader(transport_name, header);
            }
            return down_prot.down(evt);
        }

        public String getName() {
            return null;
        }

        public String toString() {
            return cluster_name + " (" + transport_name + ")";
        }
    }


}


