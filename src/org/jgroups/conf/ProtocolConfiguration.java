package org.jgroups.conf;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Parses and encapsulates the specification for 1 protocol of the protocol stack, e.g.
 * <code>UNICAST(timeout=5000)</code>
 * @author Bela Ban
 * @version $Id: ProtocolConfiguration.java,v 1.1 2010/09/15 15:48:55 belaban Exp $
 */
public class ProtocolConfiguration {
    private final String protocol_name;
    private String properties_str;
    private final Map<String, String> properties=new HashMap<String, String>();
    public static final String protocol_prefix="org.jgroups.protocols";
    public static final Log log=LogFactory.getLog(ProtocolConfiguration.class);


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
        parsePropertiesString(properties_str, properties);
    }

    public String getProtocolName() {
        return protocol_name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getOriginalProperties() throws Exception {
        Map<String, String> props=new HashMap<String, String>();
        parsePropertiesString(properties_str, props);
        return props;
    }

    private void parsePropertiesString(String properties_str, Map<String, String> properties) throws Exception {
        int index=0;

        /* "in_port=5555;out_port=6666" */
        if(properties_str.length() > 0) {
            String[] components=properties_str.split(";");
            for(String property : components) {
                String name, value;
                index=property.indexOf('=');
                if(index == -1) {
                    throw new Exception("Configurator.ProtocolConfiguration(): '=' not found in " + property
                            + " of " + protocol_name);
                }
                name=property.substring(0, index);
                value=property.substring(index + 1, property.length());
                properties.put(name, value);
            }
        }
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
                if(tmp.contains("${")) {
                    if(log.isWarnEnabled())
                        log.warn("variable \"" + val + "\" in " + protocol_name + " could not be substituted; " +
                                key + " is removed from properties");
                    it.remove();
                }
            }
        }
        properties_str=propertiesToString();
    }


    public String toString() {
        StringBuilder retval=new StringBuilder();
        if(protocol_name == null)
            retval.append("<unknown>");
        else
            retval.append(protocol_name);
        if(properties != null)
            retval.append("(" + Util.print(properties) + ')');
        return retval.toString();
    }

    public String propertiesToString() {
        return Util.printMapWithDelimiter(properties, ";");
    }


}