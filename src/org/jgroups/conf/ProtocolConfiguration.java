package org.jgroups.conf;

import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.*;


/**
 * Parses and encapsulates the specification for 1 protocol of the protocol stack, e.g. {@code UNICAST(timeout=5000)}
 * @author Bela Ban
 */
public class ProtocolConfiguration {
    private final String              protocol_name;
    private final ClassLoader         loader;
    private String                    properties_str;
    private final Map<String,String>  properties=new HashMap<>();
    private List<XmlNode>             subtrees; // root to XmlNode tree, passed to protocol on creation


    /**
     * Creates a new ProtocolConfiguration.
     * @param config_str The configuration specification for the protocol, e.g.
     *                   <pre>VERIFY_SUSPECT(timeout=1500)</pre>
     */
    public ProtocolConfiguration(String config_str) throws Exception {
        int index=config_str.indexOf('(');  // e.g. "UDP(in_port=3333)"
        int end_index=config_str.lastIndexOf(')');

        if(index == -1) {
            protocol_name=config_str;
            properties_str="";
        }
        else {
            if(end_index == -1) {
                throw new Exception("Configurator.ProtocolConfiguration(): closing ')' " +
                        "not found in " + config_str + ": properties cannot be set !");
            }
            else {
                properties_str=config_str.substring(index + 1, end_index);
                protocol_name=config_str.substring(0, index);
            }
        }
        parsePropertiesString(properties);
        this.loader = ProtocolConfiguration.class.getClassLoader();
    }

    public ProtocolConfiguration(String protocol_name, Map<String,String> properties) {
        this(protocol_name, properties, ProtocolConfiguration.class.getClassLoader());
    }

    public ProtocolConfiguration(String protocol_name, Map<String,String> properties, ClassLoader loader) {
        this.protocol_name=protocol_name;
        this.loader = loader;
        if(properties != null && !properties.isEmpty()) {
            this.properties.putAll(properties);
            properties_str=propertiesToString();
        }
    }

    public void addSubtree(XmlNode node) {
        if(node == null)
            return;
        if(subtrees == null)
            subtrees=new ArrayList<>();
        subtrees.add(node);
    }

    public List<XmlNode> getSubtrees() {
        return subtrees;
    }

    public String getProtocolName() {
        return protocol_name;
    }

    public ClassLoader getClassLoader() {
        return this.loader;
    }

    public Map<String,String> getProperties() {
        return properties;
    }

    /**
     * Load the class of the {@link Protocol} configured by this instance.
     *
     * @param loadingClass The {@link Class} to fetch the preferred {@link ClassLoader}. It can be null.
     * @return The {@link Class} of the {@link Protocol}.
     * @throws Exception If unable to find or load the class.
     */
    public Class<? extends Protocol> loadProtocolClass(Class<?> loadingClass) throws Exception {
        return Util.loadProtocolClass(protocol_name, loadingClass);
    }

    /**
     * Determines if the class {@code other} is the same or superclass (or superinterface) as the {@link Protocol}
     * configured by this instance.
     *
     * @param other        The {@link Class} to check.
     * @param loadingClass The {@link Class} to fetch the preferred {@link ClassLoader}. It can be null.
     * @return {@code true} if {@code other} is the same or superclass/interface as the {@link Protocol} configured by this instance.
     * @throws Exception If unable to find or load the class.
     */
    public boolean isAssignableProtocol(Class<? extends Protocol> other, Class<?> loadingClass) throws Exception {
        return loadProtocolClass(loadingClass).isAssignableFrom(other);
    }

    public void substituteVariables() {
        for(Iterator<Map.Entry<String, String>> it=properties.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, String> entry=it.next();
            String key=entry.getKey();
            String val=entry.getValue();
            String tmp=Util.substituteVariable(val);
            if(!val.equals(tmp)) {
                properties.put(key, tmp);
            }
            else {
                if(tmp.contains("${"))
                    it.remove();
            }
        }
        properties_str=propertiesToString();
    }


    public String toString() {
        StringBuilder retval=new StringBuilder();
        retval.append(Objects.requireNonNullElse(protocol_name, "<unknown>"));
        if(properties != null)
            retval.append("(" + Util.print(properties) + ')');
        return retval.toString();
    }

    public String propertiesToString() {
        return Util.printMapWithDelimiter(properties, ";");
    }

    public String getProtocolString(boolean new_format) {
         return new_format? getProtocolStringNewXml() : getProtocolString();
    }

    public String getProtocolString() {
        StringBuilder buf=new StringBuilder(protocol_name);
        if(!properties.isEmpty()) {
            boolean first=true;
            buf.append('(');
            for(Map.Entry<String,String> entry: properties.entrySet()) {
                String key=entry.getKey();
                String val=entry.getValue();
                if(first)
                    first=false;
                else
                    buf.append(';');
                buf.append(getParameterString(key, val));
            }
            buf.append(')');
        }
        return buf.toString();
    }


    public String getProtocolStringNewXml() {
        StringBuilder buf=new StringBuilder(protocol_name + ' ');
        if(!properties.isEmpty()) {
            boolean first=true;
            for(Map.Entry<String,String> entry: properties.entrySet()) {
                String key=entry.getKey();
                String val=entry.getValue();
                if(first)
                    first=false;
                else
                    buf.append(' ');
                buf.append(getParameterStringXml(key, val));
            }
        }
        return buf.toString();
    }

    protected static String getParameterString(String name, String value) {
        StringBuilder buf=new StringBuilder(name);
        if(value != null)
            buf.append('=').append(value);
        return buf.toString();
    }

    protected static String getParameterStringXml(String name, String val) {
        StringBuilder buf=new StringBuilder(name);
        if(val != null)
            buf.append("=\"").append(val).append('\"');
        return buf.toString();
    }



    protected void parsePropertiesString(Map<String, String> properties) throws Exception {
        int index=0;

        /* "in_port=5555;out_port=6666" */
        if(!properties_str.isEmpty()) {
            String[] components=properties_str.split(";");
            for(String property : components) {
                String name, value;
                index=property.indexOf('=');
                if(index == -1) {
                    throw new Exception("Configurator.ProtocolConfiguration(): '=' not found in " + property
                            + " of " + protocol_name);
                }
                name=property.substring(0, index);
                value=property.substring(index + 1);
                properties.put(name, value);
            }
        }
    }

}
