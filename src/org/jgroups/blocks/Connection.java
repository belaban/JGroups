package org.jgroups.blocks;

import java.io.Closeable;
import java.io.IOException;


public interface Connection extends Closeable {
    
    public boolean isOpen();
    
    public boolean isExpired(long milis);
    
    public void close() throws IOException;
    
}