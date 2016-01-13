package org.jgroups.fork;

import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.XmlConfigurator;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the fork-stacks.xsd schema. See conf/fork-stacks.xml for an example
 * @author Bela Ban
 * @since  3.4
 */
public final class ForkConfig {
    protected static final String FORK_STACKS   = "fork-stacks";
    protected static final String FORK_STACK    = "fork-stack";
    protected static final String ID            = "id";

	private ForkConfig() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    /**
     * Parses the input and returns a map of fork-stack IDs and lists of ProtocolConfigurations
     */
    public static Map<String,List<ProtocolConfiguration>> parse(InputStream input) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(input);
        Element root=document.getDocumentElement();
        return parse(root);
    }

    public static Map<String,List<ProtocolConfiguration>> parse(Node root) throws Exception {
        match(FORK_STACKS, root.getNodeName(), true);
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return null;

        Map<String,List<ProtocolConfiguration>> map=new HashMap<>();
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String element_name=node.getNodeName();
            if(FORK_STACK.equals(element_name))
                parseForkStack(map, node);
            else
                throw new Exception("expected <" + FORK_STACK + ">, but got " + "<" + element_name + ">");
        }

        return map;
    }


    protected static void parseForkStack(final Map<String,List<ProtocolConfiguration>> map, Node root) throws Exception {
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;

        NamedNodeMap attributes=root.getAttributes();
        String fork_stack_id=attributes.getNamedItem(ID).getNodeValue();
        if(map.containsKey(fork_stack_id))
            throw new IllegalStateException("duplicate fork-stack ID: \"" + fork_stack_id + "\"");

        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;

            List<ProtocolConfiguration> protocols=XmlConfigurator.parseProtocols((Element)node);
            map.put(fork_stack_id, protocols);
        }
    }



    protected static void match(String expected_name, String name, boolean is_element) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception((is_element? "Element " : "Attribute ") + "\"" + name + "\" didn't match \"" + expected_name + "\"");
    }



    public static void main(String[] args) throws Exception {
        InputStream input=new FileInputStream("/home/bela/fork-stacks.xml");
        Map<String,List<ProtocolConfiguration>> fork_stacks=parse(input);
        System.out.println("fork_stacks:");
        for(Map.Entry<String,List<ProtocolConfiguration>> entry: fork_stacks.entrySet())
            System.out.println(entry.getKey() + ":\n" + entry.getValue() + "\n");

    }
}
