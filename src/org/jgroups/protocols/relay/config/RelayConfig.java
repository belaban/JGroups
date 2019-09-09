package org.jgroups.protocols.relay.config;

import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Parses and maintains the RELAY2 configuration (in memory)
 * @author Bela Ban
 * @since 3.2
 */
public final class RelayConfig {
    protected static final String RELAY_CONFIG  = "RelayConfiguration";
    protected static final String SITES         = "sites";
    protected static final String SITE          = "site";
    protected static final String BRIDGES       = "bridges";
    protected static final String BRIDGE        = "bridge";
    protected static final String FORWARDS      = "forwards";
    protected static final String FORWARD       = "forward";
    
	private RelayConfig() {
		throw new InstantiationError( "Must not instantiate this class" );
	}


    /*public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("sites:\n");
        for(Map.Entry<String,SiteConfig> entry: sites.entrySet())
            sb.append(entry.getKey() + " --> " + entry.getValue() + "\n");

        return sb.toString();
    }*/

    /** Parses site names and their configuration (e.g. "nyc" --> SiteConfig) into the map passed as argument */
    public static void parse(InputStream input, final Map<String,SiteConfig> map) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(input);
        Element root=document.getDocumentElement();
        parse(root, map);
    }

    public static void parse(Node root, final Map<String,SiteConfig> map) throws Exception {
        match(RELAY_CONFIG, root.getNodeName(), true);
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String element_name=node.getNodeName();
            if(SITES.equals(element_name))
                parseSites(map, node);
            else
                throw new Exception("expected <" + SITES + ">, but got " + "<" + element_name + ">");
        }
    }


    protected static void parseSites(final Map<String,SiteConfig> map, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            match(SITE, node.getNodeName(), true);
            NamedNodeMap attrs=node.getAttributes();
            if(attrs == null || attrs.getLength() == 0)
                continue;
            Attr name_attr=(Attr)attrs.getNamedItem("name");
            String name=name_attr.getValue();
            if(map.containsKey(name))
                throw new Exception("Site \"" + name + "\" already defined");
            SiteConfig site_config=new SiteConfig(name);
            map.put(name, site_config);

            parseBridgesAndForwards(site_config, node);
        }
    }

    protected static void parseBridgesAndForwards(SiteConfig site_config, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String node_name=node.getNodeName();

            if(BRIDGES.equals(node_name))
                parseBridges(site_config, node);
            else if(FORWARDS.equals(node_name))
                parseForwards(site_config, node);
            else
                throw new Exception("expected \"" + BRIDGES + "\" or \"" + FORWARDS + "\" keywords");
        }
    }

    protected static void parseBridges(SiteConfig site_config, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String node_name=node.getNodeName();
            match(BRIDGE, node_name, true);

            NamedNodeMap attrs=node.getAttributes();
            if(attrs == null || attrs.getLength() == 0)
                continue;
            Attr name_attr=(Attr)attrs.getNamedItem("name");
            Attr config_attr=(Attr)attrs.getNamedItem("config");
            String name=name_attr != null? name_attr.getValue() : null;
            String config=config_attr.getValue();
            BridgeConfig bridge_config=new PropertiesBridgeConfig(name, config);
            site_config.addBridge(bridge_config);
        }
    }

    protected static void parseForwards(SiteConfig site_config, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String node_name=node.getNodeName();
            match(FORWARD, node_name, true);

            NamedNodeMap attrs=node.getAttributes();
            if(attrs == null || attrs.getLength() == 0)
                continue;
            Attr to_attr=(Attr)attrs.getNamedItem("to");
            Attr gw_attr=(Attr)attrs.getNamedItem("gateway");
            String to=to_attr.getValue();
            String gateway=gw_attr.getValue();
            ForwardConfig forward_config=new ForwardConfig(to, gateway);
            site_config.addForward(forward_config);
        }
    }


    protected static void match(String expected_name, String name, boolean is_element) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception((is_element? "Element " : "Attribute ") + "\"" + name + "\" didn't match \"" + expected_name + "\"");
    }

    public static class SiteConfig {
        protected final String              name;
        protected final List<BridgeConfig>  bridges=new ArrayList<>();
        protected final List<ForwardConfig> forwards=new ArrayList<>();

        public SiteConfig(String name) {
            this.name=name;
        }

        public String getName() {return name;}

        public List<BridgeConfig>  getBridges()   {return bridges;}
        public List<ForwardConfig> getForwards()  {return forwards;}

        public SiteConfig addBridge(BridgeConfig bridge_config)    {bridges.add(bridge_config);   return this;}
        public SiteConfig addForward(ForwardConfig forward_config) {forwards.add(forward_config); return this;}

        public String toString() {
            StringBuilder sb=new StringBuilder("name=" + name + "\n");
            if(!bridges.isEmpty())
                for(BridgeConfig bridge_config: bridges)
                    sb.append(bridge_config).append("\n");
            if(!forwards.isEmpty())
                for(ForwardConfig forward_config: forwards)
                    sb.append(forward_config).append("\n");
            return sb.toString();
        }
    }

    public abstract static class BridgeConfig {
        protected final String cluster_name;

        protected BridgeConfig(String cluster_name) {this.cluster_name=cluster_name;}

        public String            getClusterName()  {return cluster_name;}
        public abstract JChannel  createChannel() throws Exception;

        public String toString() {return "cluster=" + cluster_name;}
    }

    public static class PropertiesBridgeConfig extends BridgeConfig {
        protected final String config;

        public PropertiesBridgeConfig(String cluster_name, String config) {
            super(cluster_name);
            this.config= Util.substituteVariable(config);
        }

        public JChannel createChannel() throws Exception {return new JChannel(config);}
        public String toString() {return String.format("config=%s\n%s", config, super.toString());}
    }


    public static class ProgrammaticBridgeConfig extends BridgeConfig {
        protected Protocol[] protocols;

        public ProgrammaticBridgeConfig(String cluster_name, Protocol[] prots) {
            super(cluster_name);
            this.protocols=prots;
        }

        public JChannel createChannel() throws Exception {
            return new JChannel(protocols);
        }

        public String toString() {
            return super.toString() + ", protocols=" + printProtocols(protocols);
        }


        protected static String printProtocols(Protocol[] protocols) {
            StringBuilder sb=new StringBuilder("[");
            boolean first=true;
            for(Protocol prot: protocols) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                sb.append(prot.getName());
            }
            sb.append("]");
            return sb.toString();
        }
    }


    public static class ForwardConfig {
        protected final String to;
        protected final String gateway;

        public ForwardConfig(String to, String gateway) {
            this.to=to;
            this.gateway=gateway;
        }

        public String getGateway() {return gateway;}
        public String getTo()      {return to;}

        public String toString() {
            return "forward to=" + to + " gateway=" + gateway;
        }
    }

    public static void main(String[] args) throws Exception {
        InputStream input=new FileInputStream("/home/bela/relay2.xml");
        Map<String,SiteConfig> sites=new HashMap<>();
        RelayConfig.parse(input, sites);
        System.out.println("sites:");
        for(Map.Entry<String,SiteConfig> entry: sites.entrySet())
            System.out.println(entry.getKey() + ":\n" + entry.getValue() + "\n");

    }
}
