package org.jgroups.fork;

import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.conf.XmlNode;

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
        XmlNode root=XmlConfigurator.parseXmlDocument(input);
        return parse(root);
    }

    public static Map<String,List<ProtocolConfiguration>> parse(XmlNode root) throws Exception {
        match(FORK_STACKS, root.getName());
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return null;

        Map<String,List<ProtocolConfiguration>> map=new HashMap<>();
        for(XmlNode node: children) {
            match(FORK_STACK, node.getName());
            parseForkStack(map, node);
        }
        return map;
    }


    protected static void parseForkStack(final Map<String,List<ProtocolConfiguration>> map, XmlNode root) throws Exception {
        List<XmlNode> children=root.getChildren();
        if(children == null || children.isEmpty())
            return;

        Map<String,String> attributes=root.getAttributes();
        String fork_stack_id=attributes.get(ID);
        if(map.containsKey(fork_stack_id))
            throw new IllegalStateException("duplicate fork-stack ID: \"" + fork_stack_id + "\"");

        for(XmlNode node : children) {
            List<ProtocolConfiguration> protocols=XmlConfigurator.parseProtocols(node);
            map.put(fork_stack_id, protocols);
        }
    }



    protected static void match(String expected_name, String name) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception("\"" + name + "\" didn't match \"" + expected_name + "\"");
    }


}
