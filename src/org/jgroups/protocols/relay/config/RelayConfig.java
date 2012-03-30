package org.jgroups.protocols.relay.config;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses and maintains the RELAY2 configuration (in memory)
 * @author Bela Ban
 * @since 3.1
 */
public class RelayConfig {
    protected static final String RELAY_CONFIG  = "RelayConfiguration";
    protected static final String BRIDGES       = "bridges";
    protected static final String BRIDGE        = "bridge";
    protected static final String SITES         = "sites";
    protected static final String SITE          = "site";
    protected static final String ROUTING       = "routing";
    protected static final String ROUTE         = "route";
    protected static final String FORWARD_TO    = "forward-to";
    protected static final String DEFAULT_ROUTE = "default-route";

    /** Contains a mapping between bridge names and their configuration, e.g. "nyc" --> /home/bela/global.xml */
    protected final Map<String,String> bridges=new HashMap<String,String>(5);


    public void addBridge(String bridge_name, String config) {
        bridges.put(bridge_name,config);
    }

    public boolean containsBridge(String bridge_name) {
        return bridges.containsKey(bridge_name);
    }

    public void removeBridge(String bridge_name) {
        bridges.remove(bridge_name);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("bridges:\n");
        for(Map.Entry<String,String> entry: bridges.entrySet())
            sb.append(entry.getKey() + " --> " + entry.getValue() + "\n");

        return sb.toString();
    }

    public static RelayConfig create(InputStream input) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(input);
        Element root=document.getDocumentElement();
        match(RELAY_CONFIG, root.getNodeName(), true);
        RelayConfig retval=new RelayConfig();
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return retval;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            String element_name=node.getNodeName();
            if(BRIDGES.equals(element_name)) {
                parseBridges(retval, node);
            }
            else if(SITES.equals(element_name)) {
                parseSites(retval, node);
            }
            else
                throw new Exception("expected <" + BRIDGES + "> or <" + SITES + ">, but got " + "<" + element_name + ">");
        }

        return retval;
    }

    protected static void parseBridges(RelayConfig config, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            match(BRIDGE, node.getNodeName(), true);
            NamedNodeMap attrs=node.getAttributes();
            if(attrs == null || attrs.getLength() == 0)
                continue;
            Attr name=(Attr)attrs.getNamedItem("name");
            Attr cfg=(Attr)attrs.getNamedItem("config");
            if(config.containsBridge(name.getValue()))
                throw new Exception("Duplicate entry for bridge \"" + name.getValue() + "\"");
            config.addBridge(name.getValue(), cfg.getValue());
        }
    }

    protected static void parseSites(RelayConfig config, Node root) {

    }

    protected static void match(String expected_name, String name, boolean is_element) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception((is_element? "Element " : "Attribute ") + "\"" + name + "\" didn't match \"" + expected_name + "\"");
    }


}
