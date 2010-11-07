package org.jgroups.blocks;

import java.io.IOException;


public interface Connection{
    
    public boolean isOpen();
    
    public boolean isExpired(long milis);
    
    public void close() throws IOException;
    
}