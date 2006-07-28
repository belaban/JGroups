// $Id: JChannelFactory.java,v 1.30 2006/07/28 08:26:06 belaban Exp $

package org.jgroups;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.ClassPathEntityResolver;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * JChannelFactory creates pure Java implementations of the <code>Channel</code>
 * interface.
 * See {@link JChannel} for a discussion of channel properties.
 */
public class JChannelFactory implements ChannelFactory {
    private ProtocolStackConfigurator configurator;

    private Log log=LogFactory.getLog(getClass());

    /** Map<String,String>. Hashmap which maps stack names to JGroups configurations. Keys are stack names, values are
     * plain JGroups stack configs. This is (re-)populated whenever a setMultiplexerConfig() method is called */
    private final Map stacks=new HashMap();

    /** Map<String,Entry>, maintains mapping between stack names (e.g. "udp") and Entries, which contain a JChannel and
     * a Multiplexer */
    private final Map channels=new HashMap();

    private String config=null;

    /** The MBeanServer to expose JMX management data with (no management data will be available if null) */
    private MBeanServer server=null;

    /** To expose the channels and protocols */
    private String domain=null;

    /** Whether or not to expose channels via JMX */
    private boolean expose_channels=true;

    /** Whether to expose the factory only, or all protocols as well */
    private boolean expose_protocols=true;



    // private Log log=LogFactory.getLog(getClass());
    private final static String PROTOCOL_STACKS="protocol_stacks";
    private final static String STACK="stack";
    private static final String NAME="name";
    // private static final String DESCR="description";
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


    public void setMultiplexerConfig(Object properties) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(properties);
        if(input == null)
            throw new FileNotFoundException(properties.toString());
        try {
            parse(input);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + properties, ex);
        }
        finally {
            Util.closeInputStream(input);
        }
    }

    public void setMultiplexerConfig(File file) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(file);
        if(input == null)
            throw new FileNotFoundException(file.toString());
        try {
            parse(input);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + file.toString(), ex);
        }
        finally {
            Util.closeInputStream(input);
        }
    }

    public void setMultiplexerConfig(Element properties) throws Exception {
        parse(properties);
    }

    public void setMultiplexerConfig(URL url) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(url);
        if(input == null)
            throw new FileNotFoundException(url.toString());
        try {
            parse(input);
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + url.toString(), ex);
        }
        finally {
            Util.closeInputStream(input);
        }
    }

    public String getMultiplexerConfig() {return config;}

    public void setMultiplexerConfig(String properties) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(properties);
        if(input == null)
            throw new FileNotFoundException(properties);
        try {
            parse(input);
            this.config=properties;
        }
        catch(Exception ex) {
            throw new Exception("failed parsing " + properties, ex);
        }
        finally {
            Util.closeInputStream(input);
        }
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

    public Channel createMultiplexerChannel(String stack_name, String id) throws Exception {
        return createMultiplexerChannel(stack_name, id, false, null);
    }

    public Channel createMultiplexerChannel(String stack_name, String id, boolean register_for_state_transfer, String substate_id) throws Exception {
        if(stack_name == null || id == null)
            throw new IllegalArgumentException("stack name and service ID have to be non null");
        Entry entry;
        synchronized(channels) {
            entry=(Entry)channels.get(stack_name);
            if(entry == null) {
                entry=new Entry();
                channels.put(stack_name, entry);
            }
        }
        synchronized(entry) {
            JChannel ch=entry.channel;
            if(ch == null) {
                String props=getConfig(stack_name);
                ch=new JChannel(props);
                entry.channel=ch;
                if(expose_channels && server != null)
                    registerChannel(ch, stack_name);
            }
            Multiplexer mux=entry.multiplexer;
            if(mux == null) {
                mux=new Multiplexer(ch);
                entry.multiplexer=mux;
            }
            if(register_for_state_transfer)
                mux.registerForStateTransfer(id, substate_id);
            return mux.createMuxChannel(this, id, stack_name);
        }
    }

    private void registerChannel(JChannel ch, String stack_name) throws Exception {
        JmxConfigurator.registerChannel(ch, server, domain, stack_name, expose_protocols);
    }




    /** Unregisters everything under stack_name (including stack_name) */
    private void unregister(String name) throws Exception {
        JmxConfigurator.unregister(server, name);
    }


    public void connect(MuxChannel ch) throws ChannelException {
        Entry entry;
        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                if(entry.channel == null)
                    throw new ChannelException("channel has to be created before it can be connected");

                if(entry.multiplexer != null)
                    entry.multiplexer.addServiceIfNotPresent(ch.getId(), ch);
                
                if(!entry.channel.isConnected()) {
                    entry.channel.connect(ch.getStackName());
                    if(entry.multiplexer != null) {
                        try {
                            entry.multiplexer.fetchServiceInformation();
                        }
                        catch(Exception e) {
                            if(log.isErrorEnabled())
                                log.error("failed fetching service state", e);
                        }
                    }
                }
                if(entry.multiplexer != null) {
                    try {
                        Address addr=entry.channel.getLocalAddress();
                        entry.multiplexer.sendServiceUpMessage(ch.getId(), addr);
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failed sending SERVICE_UP message", e);
                    }
                }
            }
        }
        ch.setClosed(false);
        ch.setConnected(true);
    }


    public void disconnect(MuxChannel ch) {
        Entry entry;

        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                Multiplexer mux=entry.multiplexer;
                if(mux != null) {
                    Address addr=entry.channel.getLocalAddress();
                    try {
                        mux.sendServiceDownMessage(ch.getId(), addr);
                    }
                    catch(Exception e) {
                        if(log.isErrorEnabled())
                            log.error("failed sending SERVICE_DOWN message", e);
                    }
                    mux.disconnect(); // disconnects JChannel if all MuxChannels are in disconnected state
                }
            }
        }
    }


    public void close(MuxChannel ch) {
        Entry entry;
        String stack_name=ch.getStackName();
        boolean all_closed=false;

        synchronized(channels) {
            entry=(Entry)channels.get(stack_name);
        }
        if(entry != null) {
            synchronized(entry) {
                Multiplexer mux=entry.multiplexer;
                if(mux != null) {
                    Address addr=entry.channel.getLocalAddress();
                    if(addr != null) {
                        try {
                            mux.sendServiceDownMessage(ch.getId(), addr);
                        }
                        catch(Exception e) {
                            if(log.isErrorEnabled())
                                log.error("failed sending SERVICE_DOWN message", e);
                        }
                    }
                    all_closed=mux.close(); // closes JChannel if all MuxChannels are in closed state
                }
            }
            if(all_closed) {
                channels.remove(stack_name);
            }
            if(expose_channels && server != null) {
                try {
                    unregister(domain + ":*,cluster=" + stack_name);
                }
                catch(Exception e) {
                    log.error("failed unregistering channel " + stack_name, e);
                }
            }
        }
    }



    public void shutdown(MuxChannel ch) {
        Entry entry;
        String stack_name=ch.getStackName();
        boolean all_closed=false;

        synchronized(channels) {
            entry=(Entry)channels.get(stack_name);
            if(entry != null) {
                synchronized(entry) {
                    Multiplexer mux=entry.multiplexer;
                    if(mux != null) {
                        Address addr=entry.channel.getLocalAddress();
                        try {
                            mux.sendServiceDownMessage(ch.getId(), addr);
                        }
                        catch(Exception e) {
                            if(log.isErrorEnabled())
                                log.error("failed sending SERVICE_DOWN message", e);
                        }
                        all_closed=mux.shutdown(); // closes JChannel if all MuxChannels are in closed state

                        //mux.unregister(ch.getId());
                    }
                }
                if(all_closed) {
                    channels.remove(stack_name);
                }
                if(expose_channels && server != null) {
                    try {
                        unregister(domain + ":*,cluster=" + stack_name);
                    }
                    catch(Exception e) {
                        log.error("failed unregistering channel " + stack_name, e);
                    }
                }
            }
        }
    }

    public void open(MuxChannel ch) throws ChannelException {
        Entry entry;
        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                if(entry.channel == null)
                    throw new ChannelException("channel has to be created before it can be opened");
                if(!entry.channel.isOpen())
                    entry.channel.open();
            }
        }
        ch.setClosed(false);
        ch.setConnected(false); //  needs to be connected next
    }



    public void create() throws Exception{
        if(expose_channels) {
            server=Util.getMBeanServer();
            if(server == null)
                throw new Exception("No MBeanServer found; JChannelFactory needs to be run with an MBeanServer present, " +
                        "e.g. inside JBoss or JDK 5, or with ExposeChannel set to false");
            if(domain == null)
                domain="jgroups:name=Multiplexer";
        }
    }

    public void start() throws Exception {

    }

    public void stop() {

    }

    public void destroy() {
        synchronized(channels) {
            Entry entry;
            Map.Entry tmp;
            for(Iterator it=channels.entrySet().iterator(); it.hasNext();) {
                tmp=(Map.Entry)it.next();
                entry=(Entry)tmp.getValue();
                if(entry.multiplexer != null)
                    entry.multiplexer.closeAll();
                if(entry.channel != null)
                    entry.channel.close();

            }
            if(expose_channels && server != null) {
                try {
                    unregister(domain + ":*");
                }
                catch(Throwable e) {
                    log.error("failed unregistering domain " + domain, e);
                }
            }
            channels.clear();
        }
    }


    public String dumpConfiguration() {
        if(stacks != null) {
            return stacks.keySet().toString();
        }
        else
            return null;
    }

    public String dumpChannels() {
        if(channels == null)
            return null;
        StringBuffer sb=new StringBuffer();
        for(Iterator it=channels.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(((Entry)entry.getValue()).multiplexer.getServiceIds()).append("\n");
        }
        return sb.toString();
    }



    private void parse(InputStream input) throws Exception {
        /**
         * CAUTION: crappy code ahead ! I (bela) am not an XML expert, so the code below is pretty amateurish...
         * But it seems to work, and it is executed only on startup, so no perf loss on the critical path.
         * If somebody wants to improve this, please be my guest.
         */
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); //for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        builder.setEntityResolver(new ClassPathEntityResolver());
        Document document=builder.parse(input);

        // The root element of the document should be the "config" element,
        // but the parser(Element) method checks this so a check is not
        // needed here.
        Element configElement = document.getDocumentElement();
        parse(configElement);
    }

    private void parse(Element root) throws Exception {
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
                String val=conf.getProtocolStackString();
                this.stacks.put(st_name, val);
            }
            //System.out.println(" - OK");
        }
//        System.out.println("stacks: ");
//        for(Iterator it=stacks.entrySet().iterator(); it.hasNext();) {
//            Map.Entry entry=(Map.Entry)it.next();
//            System.out.println("key: " + entry.getKey());
//            System.out.println("val: " + entry.getValue() + "\n");
//        }
    }

    /**
     * Returns the stack configuration as a string (to be fed into new JChannel()). Throws an exception
     * if the stack_name is not found. One of the setMultiplexerConfig() methods had to be called beforehand
     * @return The protocol stack config as a plain string
     */
    private String getConfig(String stack_name) throws Exception {
        String cfg=(String)stacks.get(stack_name);
        if(cfg == null)
            throw new Exception("stack \"" + stack_name + "\" not found in " + stacks.keySet());
        return cfg;
    }


    private static class Entry {
        JChannel channel;
        Multiplexer multiplexer;
    }


}
