// $Id: ProtocolStackConfigurator.java,v 1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.conf;

/**
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */

public interface ProtocolStackConfigurator
{
    String         getProtocolStackString();
    ProtocolData[] getProtocolStack();
}
