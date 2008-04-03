package org.jgroups.util;

public interface ThreadFactory extends java.util.concurrent.ThreadFactory {   
    Thread newThread(Runnable r,String name);    
    Thread newThread(ThreadGroup group, Runnable r,String name);
}
