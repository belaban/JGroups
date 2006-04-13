// $Id: JChannelFactory.java,v 1.14 2006/04/13 08:45:42 belaban Exp $

package org.jgroups;

import org.jgroups.conf.ClassPathEntityResolver;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.util.Util;
import org.jgroups.mux.Multiplexer;
import org.jgroups.mux.MuxChannel;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

/**
 * JChannelFactory creates pure Java implementations of the <code>Channel</code>
 * interface.
 * See {@link JChannel} for a discussion of channel properties.
 */
public class JChannelFactory implements ChannelFactory {
    private ProtocolStackConfigurator configurator;

    /** Map<String,String>. Hashmap which maps stack names to JGroups configurations. Keys are stack names, values are
     * plain JGroups stack configs. This is (re-)populated whenever a setMultiplexerConfig() method is called */
    private final Map stacks=new HashMap();

    /** Map<String,Entry>, maintains mapping between stack names (e.g. "udp") and Entries, which contain a JChannel and
     * a Multiplexer */
    private final Map channels=new HashMap();

    // private Log log=LogFactory.getLog(getClass());
    private final static String PROTOCOL_STACKS="protocol_stacks";
    private final static String STACK="stack";
    private static final String NAME="name";
    private static final String DESCR="description";
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
        try {
            parse(input);
        }
        finally {
            Util.closeInputStream(input);
        }
    }

    public void setMultiplexerConfig(File file) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(file);
        try {
            parse(input);
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
        try {
            parse(input);
        }
        finally {
            Util.closeInputStream(input);
        }
    }

    public void setMultiplexerConfig(String properties) throws Exception {
        InputStream input=ConfiguratorFactory.getConfigStream(properties);
        try {
            parse(input);
        }
        finally {
            Util.closeInputStream(input);
        }
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
            throw new IllegalArgumentException("stack name and application ID have to be non null");
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



    public void connect(MuxChannel ch) throws ChannelException {
        Entry entry;
        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                if(entry.channel == null)
                    throw new ChannelException("channel has to be created before it can be connected");
                if(!entry.channel.isConnected())
                    entry.channel.connect(ch.getStackName());
            }
        }
        ch.setClosed(false);
        ch.setConnected(true);
        // entry.multiplexer.checkForStateTransfer(ch.getId());
    }


    public void disconnect(MuxChannel ch) {
        Entry entry;
        ch.setClosed(false);
        ch.setConnected(false);

        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                Multiplexer mux=entry.multiplexer;
                if(mux != null) {
                    mux.disconnect(); // disconnects JChannel if all MuxChannels are in disconnected state
                }
            }
        }
    }


    public void close(MuxChannel ch) {
        Entry entry;
        ch.setClosed(true);
        ch.setConnected(false);
        ch.closeMessageQueue(true);

        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                Multiplexer mux=entry.multiplexer;
                if(mux != null) {
                   mux.close(); // closes JChannel if all MuxChannels are in closed state
                }
            }
            synchronized(channels) {
                channels.remove(entry);
            }
        }
    }

    public void shutdown(MuxChannel ch) {
        Entry entry;
        ch.setClosed(true);
        ch.setConnected(false);
        ch.closeMessageQueue(true);

        synchronized(channels) {
            entry=(Entry)channels.get(ch.getStackName());
        }
        if(entry != null) {
            synchronized(entry) {
                Multiplexer mux=entry.multiplexer;
                if(mux != null) {
                   mux.shutdown(); // closes JChannel if all MuxChannels are in closed state
                }
            }
            synchronized(channels) {
                channels.remove(entry);
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

    }

    public void start() throws Exception {

    }

    public void stop() {

    }

    public void destroy() {
        synchronized(channels) {
            Entry entry;
            for(Iterator it=channels.values().iterator(); it.hasNext();) {
                entry=(Entry)it.next();
                if(entry.multiplexer != null)
                    entry.multiplexer.closeAll();
                if(entry.channel != null)
                    entry.channel.close();
            }
            channels.clear();
        }
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
            Node descr=attrs.getNamedItem(DESCR);
            String st_name=name.getNodeValue();
            String stack_descr=descr.getNodeValue();
            //System.out.print("Parsing \"" + st_name + "\" (" + stack_descr + ")");
            NodeList configs=stack.getChildNodes();
            for(int j=0; j < configs.getLength(); j++) {
                Node config=configs.item(j);
                if(config.getNodeType() != Node.ELEMENT_NODE )
                    continue;
                Element cfg = (Element) config;
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
        String config=(String)stacks.get(stack_name);
        if(config == null)
            throw new Exception("stack \"" + stack_name + "\" not found in " + stacks.keySet());
        return config;
    }


    private static class Entry {
        JChannel channel;
        Multiplexer multiplexer;
    }


}
