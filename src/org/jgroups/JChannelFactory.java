// $Id: JChannelFactory.java,v 1.57 2009/07/07 06:09:03 belaban Exp $

package org.jgroups;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.mux.Multiplexer;
import org.jgroups.mux.MuxChannel;
import org.jgroups.util.Util;
import org.w3c.dom.*;

import javax.management.MBeanServer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

/**
 * JChannelFactory creates pure Java implementations of the <code>Channel</code>
 * interface.
 * See {@link JChannel} for a discussion of channel properties.
 * @see JChannelFactory
 * @deprecated Might get removed in 3.0. Use your own method of injecting channels
 */
@Deprecated
@MBean(description="Factory to create channels")
public class JChannelFactory implements ChannelFactory {
    private ProtocolStackConfigurator configurator;

    private Log log=LogFactory.getLog(getClass());

    /**
     * Map<String,String>. Hashmap which maps stack names to JGroups
     * configurations. Keys are stack names, values are plain JGroups stack
     * configs. This is (re-)populated whenever a setMultiplexerConfig() method
     * is called
     */
    private final Map<String,String> stacks=Collections.synchronizedMap(new HashMap<String,String>());

    /**
     * Map<String,Multiplexer>, maintains mapping between stack names (e.g.
     * "udp") and Multiplexer(es)
     * 
     */
    private final Map<String,Multiplexer> channels=Collections.synchronizedMap(new HashMap<String,Multiplexer>());

    /**
     * The MBeanServer to expose JMX management data with (no management data
     * will be available if null)
     */
    private MBeanServer server=null;

    /** To expose the channels and protocols */
    @ManagedAttribute(writable=true)
    private String domain="jgroups";

    /** Whether or not to expose channels via JMX */
    @ManagedAttribute(description="Expose channels via JMX", writable=true)
    private boolean expose_channels=true;

    /** Whether to expose the factory only, or all protocols as well */
    @ManagedAttribute(description="Expose protocols via JMX", writable=true)
    private boolean expose_protocols=true;

    
    private final static String PROTOCOL_STACKS="protocol_stacks";
    private final static String STACK="stack";
    private static final String NAME="name";
    private static final String CONFIG="config";

    /**
     * Constructs a <code>JChannelFactory</code> instance that contains no
     * protocol stack configuration.
     */
    public JChannelFactory() {
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a file containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(File properties) throws ChannelException {
        configurator=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a XML element containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(Element properties) throws ChannelException {
        configurator=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a URL pointing to a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(URL properties) throws ChannelException {
        configurator=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration based upon the specified properties parameter.
     *
     * @param properties an old style property string, a string representing a
     *                   system resource containing a JGroups XML configuration,
     *                   a string representing a URL pointing to a JGroups XML
     *                   XML configuration, or a string representing a file name
     *                   that contains a JGroups XML configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(String properties) throws ChannelException {
        configurator=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @throws Exception
     */
    public void setMultiplexerConfig(Object properties) throws Exception {
        setMultiplexerConfig(properties, true);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @param replace
     * @throws Exception
     */
    public void setMultiplexerConfig(Object properties, boolean replace) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(properties);
        if(input == null)
            throw new FileNotFoundException(properties.toString());
        try {
            parse(input, replace);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + properties, ex);
        }
        finally {
            Util.close(input);
        }
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param file
     * @throws Exception
     */
    public void setMultiplexerConfig(File file) throws Exception {
        setMultiplexerConfig(file, true);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param file
     * @param replace
     * @throws Exception
     */
    public void setMultiplexerConfig(File file, boolean replace) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(file);
        if(input == null)
            throw new FileNotFoundException(file.toString());
        try {
            parse(input, replace);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + file.toString(), ex);
        }
        finally {
            Util.close(input);
        }
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @throws Exception
     */
    public void setMultiplexerConfig(Element properties) throws Exception {
        parse(properties, true);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @param replace
     * @throws Exception
     */
    public void setMultiplexerConfig(Element properties, boolean replace) throws Exception {
        parse(properties, replace);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param url
     * @throws Exception
     */
    public void setMultiplexerConfig(URL url) throws Exception {
        setMultiplexerConfig(url, true);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param url
     * @param replace
     * @throws Exception
     */
    public void setMultiplexerConfig(URL url, boolean replace) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(url);
        if(input == null)
            throw new FileNotFoundException(url.toString());
        try {
            parse(input, replace);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + url.toString(), ex);
        }
        finally {
            Util.close(input);
        }
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @throws Exception
     */
    @ManagedOperation
    public void setMultiplexerConfig(String properties) throws Exception {
        setMultiplexerConfig(properties, true);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param properties
     * @param replace
     * @throws Exception
     */
    @ManagedOperation
    public void setMultiplexerConfig(String properties, boolean replace) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(properties);
        if(input == null)
            throw new FileNotFoundException(properties);
        try {
            parse(input, replace);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + properties, ex);
        }
        finally {
            Util.close(input);
        }
    }

     /**
     * Returns the stack configuration as a string (to be fed into new JChannel()). Throws an exception
     * if the stack_name is not found. One of the setMultiplexerConfig() methods had to be called beforehand
     * @return The protocol stack config as a plain string
     */
    @ManagedOperation
    public String getConfig(String stack_name) throws Exception {
        String cfg=stacks.get(stack_name);
        if(cfg == null)
            throw new Exception("stack \"" + stack_name + "\" not found in " + stacks.keySet());
        return cfg;
     }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @return Returns all configurations
     */
    @ManagedOperation(description="Returns all configurations")
    public String getMultiplexerConfig() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,String> entry: stacks.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    /** Removes all configurations */
    @ManagedOperation(description="Removes all configurations")
    public void clearConfigurations() {
        stacks.clear();
    }

    public boolean removeConfig(String stack_name) {
        return stack_name != null && stacks.remove(stack_name) != null;
    }

    public MBeanServer getServer() {
        return server;
    }

    public void setServer(MBeanServer server) {
        this.server=server;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain=domain;
    }

    public boolean isExposeChannels() {
        return expose_channels;
    }

    public void setExposeChannels(boolean expose_channels) {
        this.expose_channels=expose_channels;
    }

    public boolean isExposeProtocols() {
        return expose_protocols;
    }

    public void setExposeProtocols(boolean expose_protocols) {
        this.expose_protocols=expose_protocols;
        if (expose_protocols)
           this.expose_channels=true;
    }


    /**
     * Creates a <code>JChannel</code> implementation of the
     * <code>Channel</code> interface.
     *
     * @param properties the protocol stack configuration information; a
     *                   <code>null</code> value means use the default protocol
     *                   stack configuration.
     *
     * @throws ChannelException if the creation of the channel failed.
     *
     * @deprecated <code>JChannel</code>'s conversion to type-specific
     *             construction, and the subsequent deprecation of its
     *             <code>JChannel(Object)</code> constructor, necessitate the
     *             deprecation of this factory method as well.  Type-specific
     *             protocol stack configuration should be specfied during
     *             construction of an instance of this factory.
     */
    public Channel createChannel(Object properties) throws ChannelException {
        return new JChannel(properties);
    }

    /**
     * Creates a <code>JChannel</code> implementation of the
     * <code>Channel<code> interface using the protocol stack configuration
     * information specfied during construction of an instance of this factory.
     *
     * @throws ChannelException if the creation of the channel failed.
     */
    public Channel createChannel() throws ChannelException {
        return new JChannel(configurator);
    }

    public Channel createChannel(String stack_name) throws Exception {
        String props=stack_name != null? getConfig(stack_name) : null;
        return new JChannel(props);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param stack_name
     * @param id
     * @return
     * @throws Exception
     */
    @ManagedOperation(description="Create multiplexed channel")
    public Channel createMultiplexerChannel(String stack_name, String id) throws Exception {
        return createMultiplexerChannel(stack_name, id, false, null);
    }

    /**
     * @deprecated Use a shared transport instead of the multiplexer
     * @param stack_name
     * @param id
     * @param register_for_state_transfer
     * @param substate_id
     * @return
     * @throws Exception
     */
    @ManagedOperation(description="Create multiplexed channel with state transfer reguistration")
    public Channel createMultiplexerChannel(final String stack_name,
                                            String id,
                                            boolean register_for_state_transfer,
                                            String substate_id) throws Exception {
        if(stack_name == null || id == null)
            throw new IllegalArgumentException("stack name and service ID have to be non null");
        
        if(stack_name.length()==0 || id.length() == 0)
            throw new IllegalArgumentException("stack name and service ID have to non empty strings");
               
        Multiplexer mux = null;
        synchronized (channels) {
            if (!channels.containsKey(stack_name)) {
                JChannel ch = new JChannel(getConfig(stack_name));
                registerChannel(ch, stack_name);
                mux = new Multiplexer(ch);
                channels.put(stack_name, mux);
            } else {
                mux = channels.get(stack_name);
            }
        }
        if(register_for_state_transfer)
            mux.registerForStateTransfer(id, substate_id);
        
        Channel c = mux.createMuxChannel(id, stack_name);
        c.addChannelListener(new MuxFactoryChannelListener());
        return c;
    }
    
    /**
     * Returns true if this factory has already registered MuxChannel with given
     * stack_name and an id, false otherwise.
     * @deprecated Use a shared transport instead of the multiplexer
     * @param stack_name
     *            name of the stack used
     * @param id
     *            service id
     * @return true if such MuxChannel exists, false otherwise
     */
   public boolean hasMuxChannel(String stack_name, String id) {
		Multiplexer entry = channels.get(stack_name);
		if (entry != null) {
			Set<String> services = entry.getServiceIds();
			return (services != null && services.contains(id));
		}
		return false;
	}
   
    public void create() throws Exception{
        if(expose_channels) {
            if(server == null)
                server=Util.getMBeanServer();
            if(server == null)
                throw new Exception("No MBeanServer found; JChannelFactory needs to be run with an MBeanServer present, " +
                        "e.g. inside JBoss or JDK 5, or with ExposeChannel set to false");
            if(domain == null)
                domain="jgroups";
        }
    }

    @ManagedOperation    
    public void start() throws Exception {

    }

    @ManagedOperation
    public void stop() {

    }

    @ManagedOperation
    public void destroy() {        
        synchronized (channels) {
            for(Map.Entry<String,Multiplexer> entry: channels.entrySet()){                
                Multiplexer m = entry.getValue();
                if(m != null){
                    m.closeAll();
                    m.close();
                }           
            }    
        }        
        unregister(domain + ":*");        
        channels.clear();
    }


    @ManagedOperation
    public String dumpConfiguration() {
        return stacks.keySet().toString();
    }

    @ManagedOperation
    public String dumpChannels() {
        StringBuilder sb = new StringBuilder();
        synchronized (channels) {
            for (Map.Entry<String, Multiplexer> entry : channels.entrySet()) {                
                Multiplexer m = entry.getValue();
                sb.append(entry.getKey()).append(": ").append(m.getServiceIds()).append("\n");
            }
        }
        return sb.toString();
    }
    
    private void registerChannel(JChannel ch, String stack_name) throws Exception {
        if(expose_channels && server != null)
            JmxConfigurator.registerChannel(ch, server, domain, stack_name, expose_protocols);
    }

    
    private void unregister(String name) {
        if(expose_channels && server != null){
            try{
                JmxConfigurator.unregister(server, name);
            }catch(Exception e){
                log.error("failed unregistering " + name, e);
            }
        }
    }



    private void parse(InputStream input, boolean replace) throws Exception {
        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
         * But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
         * If somebody wants to improve this, please be my guest.
         */
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); //for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(input);

        // The root element of the document should be the "config" element,
        // but the parser(Element) method checks this so a check is not
        // needed here.
        Element configElement = document.getDocumentElement();
        parse(configElement, replace);
    }

    private void parse(Element root, boolean replace) throws Exception {
        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
         * But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
         * If somebody wants to improve this, please be my guest.
         */
        String root_name=root.getNodeName();
        if(!PROTOCOL_STACKS.equals(root_name.trim().toLowerCase())) {
            String error="XML protocol stack configuration does not start with a '<config>' element; " +
                    "maybe the XML configuration needs to be converted to the new format ?\n" +
                    "use 'java org.jgroups.conf.XmlConfigurator <old XML file> -new_format' to do so";
            throw new IOException("invalid XML configuration: " + error);
        }

        NodeList tmp_stacks=root.getChildNodes();
        for(int i=0; i < tmp_stacks.getLength(); i++) {
            Node node = tmp_stacks.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE )
                continue;

            Element stack=(Element) node;
            String tmp=stack.getNodeName();
            if(!STACK.equals(tmp.trim().toLowerCase())) {
                throw new IOException("invalid configuration: didn't find a \"" + STACK + "\" element under \"" + PROTOCOL_STACKS + "\"");
            }

            NamedNodeMap attrs = stack.getAttributes();
            Node name=attrs.getNamedItem(NAME);
            // Node descr=attrs.getNamedItem(DESCR);
            String st_name=name.getNodeValue();
            // String stack_descr=descr.getNodeValue();
            // System.out.print("Parsing \"" + st_name + "\" (" + stack_descr + ")");
            NodeList configs=stack.getChildNodes();
            for(int j=0; j < configs.getLength(); j++) {
                Node tmp_config=configs.item(j);
                if(tmp_config.getNodeType() != Node.ELEMENT_NODE )
                    continue;
                Element cfg = (Element) tmp_config;
                tmp=cfg.getNodeName();
                if(!CONFIG.equals(tmp))
                    throw new IOException("invalid configuration: didn't find a \"" + CONFIG + "\" element under \"" + STACK + "\"");

                XmlConfigurator conf=XmlConfigurator.getInstance(cfg);
                // fixes http://jira.jboss.com/jira/browse/JGRP-290
                ConfiguratorFactory.substituteVariables(conf); // replace vars with system props
                String val=conf.getProtocolStackString();
                if(replace) {
                    stacks.put(st_name, val);
                    if(log.isTraceEnabled())
                        log.trace("added config '" + st_name + "'");
                }
                else {
                    if(!stacks.containsKey(st_name)) {
                        stacks.put(st_name, val);
                        if(log.isTraceEnabled())
                            log.trace("added config '" + st_name + "'");
                    }
                    else {
                        if(log.isTraceEnabled())
                            log.trace("didn't add config '" + st_name + " because one of the same name already existed");
                    }
                }
            }
        }
    }


    
    private class MuxFactoryChannelListener extends ChannelListenerAdapter{

        public void channelClosed(Channel channel) {
            MuxChannel mch = (MuxChannel)channel;
            Multiplexer multiplexer = mch.getMultiplexer();
            boolean all_closed = multiplexer.close();
            if(all_closed) {
                channels.remove(mch.getStackName());
                unregister(domain + ":*,cluster=" + mch.getStackName());
            }
        }            
    }
}
