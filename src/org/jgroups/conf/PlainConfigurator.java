// $Id: PlainConfigurator.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.conf;

/**
 * A ProtocolStackConfigurator for the old style properties.
 * <BR>
 * Old style properties are referred to as the property string used by channel earlier
 * they look like this PROTOCOL(param=value;param=value):PROTOCOL:PROTOCOL<BR>
 * All it does is that it holds the string, it currently doesn't parse it at all.
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */

public class PlainConfigurator implements ProtocolStackConfigurator
{
    private String mProperties;
    
    /**
     * Instantiates a PlainConfigurator with old style properties
     */
    public PlainConfigurator(String properties)
    {
        mProperties = properties;
    }
    
    /**
     * returns the old style protocol string
     */
    public String getProtocolStackString()
    {
        return mProperties;
    }
    
    /**
     * Throws a UnsupportedOperationException all the time. No parsing implemented.
     */
    public ProtocolData[] getProtocolStack()
    {
        /**todo: Implement this org.jgroups.conf.ProtocolStackConfigurator method*/
        throw new java.lang.UnsupportedOperationException("Method getProtocolStack() not yet implemented.");
    }
}
