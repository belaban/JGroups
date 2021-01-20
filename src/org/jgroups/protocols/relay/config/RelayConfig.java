package org.jgroups.protocols.relay.config;

import org.jgroups.JChannel;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.conf.XmlNode;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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



    /** Parses site names and their configuration (e.g. "nyc" --> SiteConfig) into the map passed as argument */
    public static void parse(InputStream input, final Map<String,SiteConfig> map) throws Exception {
        XmlNode root=XmlConfigurator.parseXmlDocument(input);
        parse(root, map);
    }

    public static void parse(XmlNode root, final Map<String,SiteConfig> map) throws Exception {
        match(RELAY_CONFIG, root.getName());
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;
        for(XmlNode node: children) {
            match(SITES, node.getName());
            parseSites(map, node);
        }
    }


    protected static void parseSites(final Map<String,SiteConfig> map, XmlNode root) throws Exception {
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;
        for(XmlNode node: children) {
            match(SITE, node.getName());
            Map<String,String> attrs=node.getAttributes();
            if(attrs == null || attrs.isEmpty() || !attrs.containsKey("name"))
                throw new IllegalStateException(String.format("site must have a name (attrs: %s)", attrs));
            String site_name=attrs.get("name");
            if(map.containsKey(site_name))
                throw new Exception("Site \"" + site_name + "\" already defined");
            SiteConfig site_config=new SiteConfig(site_name);
            map.put(site_name, site_config);
            parseBridgesAndForwards(site_config, node);
        }
    }

    protected static void parseBridgesAndForwards(SiteConfig site_config, XmlNode root) throws Exception {
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;
        for(XmlNode node: children) {
            String node_name=node.getName();
            if(BRIDGES.equals(node_name))
                parseBridges(site_config, node);
            else if(FORWARDS.equals(node_name))
                parseForwards(site_config, node);
            else
                throw new Exception("expected \"" + BRIDGES + "\" or \"" + FORWARDS + "\" keywords");
        }
    }

    protected static void parseBridges(SiteConfig site_config, XmlNode root) throws Exception {
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;
        for(XmlNode node: children) {
            String node_name=node.getName();
            match(BRIDGE, node_name);

            Map<String,String> attrs=node.getAttributes();
            if(attrs == null || attrs.isEmpty())
                continue;
            String name=attrs.get("name");
            String config=attrs.get("config");
            BridgeConfig bridge_config=new PropertiesBridgeConfig(name, config);
            site_config.addBridge(bridge_config);
        }
    }

    protected static void parseForwards(SiteConfig site_config, XmlNode root) throws Exception {
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;
        for(XmlNode node: children) {
            match(FORWARD, node.getName());
            Map<String,String> attrs=node.getAttributes();
            if(attrs == null || attrs.isEmpty())
                continue;
            String to=attrs.get("to");
            String gateway=attrs.get("gateway");
            ForwardConfig forward_config=new ForwardConfig(to, gateway);
            site_config.addForward(forward_config);
        }
    }


    protected static void match(String expected_name, String name) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception("\"" + name + "\" didn't match \"" + expected_name + "\"");
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
            this.config=Util.substituteVariable(config);
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
