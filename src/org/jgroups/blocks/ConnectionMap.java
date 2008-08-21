package org.jgroups.blocks;

import org.jgroups.Address;

public interface ConnectionMap<V extends Connection> {
    
    V getConnection(Address dest) throws Exception;            
    
}
