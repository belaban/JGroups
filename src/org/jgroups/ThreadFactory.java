package org.jgroups;

public interface ThreadFactory extends java.util.concurrent.ThreadFactory{   
    Thread newThread(Runnable r,String name);        
}
