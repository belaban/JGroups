
package org.jgroups.conf;

import java.util.List;

/**
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @author Bela Ban
 * @version 1.0
 */

public interface ProtocolStackConfigurator {
    String                      getProtocolStackString();
    List<ProtocolConfiguration> getProtocolStack();
}
