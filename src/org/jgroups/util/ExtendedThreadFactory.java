package org.jgroups.util;

/**
 * Interface which makes naming patterns, address, cluster name and renaming operations visible
 * @author Bela Ban
 * @version $Id: ExtendedThreadFactory.java,v 1.2 2008/05/23 05:33:36 belaban Exp $
 */
public interface ExtendedThreadFactory extends ThreadFactory {
    void setPattern(String pattern);
    void setIncludeClusterName(boolean includeClusterName);
    void setClusterName(String channelName);
    void setAddress(String address);
    // Thread newThread(ThreadGroup group, Runnable r, String name, String address, String cluster_name);
    void renameThread(String base_name, Thread thread);
    void renameThread(String base_name, Thread thread, String address, String cluster_name);
    // void renameThread(Thread thread, String address, String cluster_name);
    void renameThread(Thread thread);
}
