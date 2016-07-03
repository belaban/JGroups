package org.jgroups.util;

public interface ThreadFactory extends java.util.concurrent.ThreadFactory {
    Thread newThread(Runnable r, String name);

    void setPattern(String pattern);
    void setIncludeClusterName(boolean includeClusterName);
    void setClusterName(String channelName);
    void setAddress(String address);   
    void renameThread(String base_name, Thread thread);   
}
