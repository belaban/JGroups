package org.jgroups.util;

/**
 * Thread factory mainly responsible for naming of threads. Can be replaced by
 * user. If use_numbering is set, a thread THREAD will be called THREAD-1,
 * THREAD-2, and so on.<p/> If a pattern has been set (through setPattern()),
 * then the cluster name and local address will also be added, e.g.
 * THREAD-5,MyCluster,192.168.1.5:63754 or THREAD,MyCluster,192.168.1.5:63754
 * 
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @version $Id: DefaultThreadFactory.java,v 1.3.2.4 2008/05/26 09:14:38 belaban
 *          Exp $
 */
public class DefaultThreadFactory implements ThreadFactory, ThreadManager {
    protected final ThreadGroup group;
    protected final String baseName;
    protected final boolean createDaemons;
    protected short counter=0; // if numbering is enabled
    protected final boolean use_numbering;

    protected boolean includeClusterName=false;
    protected boolean includeLocalAddress=false;
    protected String clusterName=null;
    protected String address=null;
    protected ThreadDecorator threadDecorator=null;

    public DefaultThreadFactory(ThreadGroup group,String baseName,boolean createDaemons) {
        this(group, baseName, createDaemons, false);
    }

    public DefaultThreadFactory(ThreadGroup group,
                                String baseName,
                                boolean createDaemons,
                                boolean use_numbering) {
        this.group=group;
        this.baseName=baseName;
        this.createDaemons=createDaemons;
        this.use_numbering=use_numbering;
    }

    public void setPattern(String pattern) {
        if(pattern != null) {
            includeClusterName=pattern.contains("c");
            includeLocalAddress=pattern.contains("l");
        }
    }

    public void setIncludeClusterName(boolean includeClusterName) {
        this.includeClusterName=includeClusterName;
    }

    public void setClusterName(String channelName) {
        clusterName=channelName;
    }

    public void setAddress(String address) {
        this.address=address;
    }

    public ThreadDecorator getThreadDecorator() {
        return threadDecorator;
    }

    public void setThreadDecorator(ThreadDecorator threadDecorator) {
        this.threadDecorator=threadDecorator;
    }

    public Thread newThread(Runnable r, String name) {
        return newThread(group, r, name);
    }

    public Thread newThread(Runnable r) {
        return newThread(group, r, baseName);
    }

    public Thread newThread(ThreadGroup group, Runnable r, String name) {
        return newThread(group, r, name, null, null);
    }

    protected Thread newThread(ThreadGroup group,
                               Runnable r,
                               String name,
                               String address,
                               String cluster_name) {
        Thread retval=new Thread(group, r, name);
        retval.setDaemon(createDaemons);
        renameThread(retval, address, cluster_name);
        if(threadDecorator != null)
            threadDecorator.threadCreated(retval);
        return retval;
    }

    public void renameThread(String base_name, Thread thread) {
        renameThread(base_name, thread, address, clusterName);
    }

    public void renameThread(String base_name, Thread thread, String address, String cluster_name) {
        if(thread == null)
            return;
        StringBuilder sb=new StringBuilder(base_name != null? base_name : thread.getName());
        if(use_numbering) {
            short id;
            synchronized(this) {
                id=++counter;
            }
            sb.append("-" + id);
        }
        if(includeClusterName) {
            sb.append(',');
            if(cluster_name != null)
                sb.append(cluster_name);
            else
                sb.append(this.clusterName);
        }

        if(includeLocalAddress) {
            sb.append(',');
            if(address != null)
                sb.append(address);
            else
                sb.append(this.address);
        }

        if(use_numbering || includeClusterName || includeLocalAddress)
            thread.setName(sb.toString());
    }

    protected void renameThread(Thread thread, String address, String cluster_name) {
        renameThread(null, thread, address, cluster_name);
    }

    public void renameThread(Thread thread) {
        renameThread(null, thread);
    }
}
