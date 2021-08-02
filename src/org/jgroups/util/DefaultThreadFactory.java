package org.jgroups.util;

import org.jgroups.logging.Log;

/**
 * Thread factory mainly responsible for naming of threads. Can be replaced by
 * user. If use_numbering is set, a thread THREAD will be called THREAD-1,
 * THREAD-2, and so on.<p/> If a pattern has been set (through setPattern()),
 * then the cluster name and local address will also be added, e.g.
 * THREAD-5,MyCluster,192.168.1.5:63754 or THREAD,MyCluster,192.168.1.5:63754
 * <p/>
 * If includeClusterName and includeLocalAddress are both false, and clusterName is set, then we assume we
 * have a shared transport, and therefore print shared=clusterName.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class DefaultThreadFactory implements ThreadFactory {
    protected final String    baseName;
    protected final boolean   createDaemons;
    protected final boolean   use_numbering;
    protected short           counter; // if numbering is enabled
    protected boolean         includeClusterName;
    protected String          clusterName;
    protected boolean         includeLocalAddress;
    protected String          address;
    protected boolean         use_fibers; // use fibers instead of threads (requires Java 15)
    protected Log             log;


    public DefaultThreadFactory(String baseName, boolean createDaemons) {
        this(baseName, createDaemons, false);
    }

    public DefaultThreadFactory(String baseName, boolean createDaemons, boolean use_numbering) {
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

    public boolean                            useFibers()          {return use_fibers;}
    public <T extends DefaultThreadFactory> T useFibers(boolean f) {this.use_fibers=f; return (T)this;}

    public <T extends DefaultThreadFactory> T log(Log l)           {this.log=l; return (T)this;}

    public Thread newThread(Runnable r, String name) {
        return newThread(r, name, null, null);
    }

    public Thread newThread(Runnable r) {
        return newThread(r, baseName, null, null);
    }

    protected Thread newThread(Runnable r, String name, String addr, String cluster_name) {
        String thread_name=getNewThreadName(name, addr, cluster_name);
        if(use_fibers)
            return Util.createFiber(r, name);
        else {
            Thread retval=use_fibers? Util.createFiber(r, name) : new Thread(r, thread_name);
            retval.setDaemon(createDaemons);
            return retval;
        }
    }

    public void renameThread(String base_name, Thread thread) {
        renameThread(base_name, thread, address, clusterName);
    }

    /**
     * Names a thread according to base_name, cluster name and local address. If includeClusterName and includeLocalAddress
     * are null, but cluster_name is set, then we assume we have a shared transport and name the thread shared=clusterName.
     * In the latter case, clusterName points to the singleton_name of TP.
     * @param base_name
     * @param thread
     * @param addr
     * @param cluster_name
     */
    public void renameThread(String base_name, Thread thread, String addr, String cluster_name) {
        String thread_name=getThreadName(base_name, thread, addr, cluster_name);
        if(thread_name != null)
            thread.setName(thread_name);
    }

    public void renameThread(Thread thread) {
        renameThread(null, thread);
    }


    protected String getThreadName(String base_name, final Thread thread, String addr, String cluster_name) {
        if(thread == null)
            return null;
        StringBuilder sb=new StringBuilder(base_name != null? base_name : thread.getName());
        if(use_numbering) {
            short id;
            synchronized(this) {
                id=++counter;
            }
            sb.append("-").append(id);
        }

        if(cluster_name == null)
            cluster_name=clusterName;
        if(addr == null)
            addr=this.address;

        if(!includeClusterName && !includeLocalAddress && cluster_name != null) {
            sb.append(",shared=").append(cluster_name);
            return sb.toString();
        }

        if(includeClusterName)
            sb.append(',').append(cluster_name);

        if(includeLocalAddress)
            sb.append(',').append(addr);

        if(use_numbering || includeClusterName || includeLocalAddress)
            return sb.toString();
        return null;
    }

    protected String getNewThreadName(String base_name, String addr, String cluster_name) {
        StringBuilder sb=new StringBuilder(base_name != null? base_name : "thread");
        if(use_numbering) {
            short id;
            synchronized(this) {
                id=++counter;
            }
            sb.append("-").append(id);
        }

        if(cluster_name == null)
            cluster_name=clusterName;
        if(addr == null)
            addr=this.address;

        if(!includeClusterName && !includeLocalAddress && cluster_name != null) {
            sb.append(",shared=").append(cluster_name);
            return sb.toString();
        }

        if(includeClusterName)
            sb.append(',').append(cluster_name);

        if(includeLocalAddress)
            sb.append(',').append(addr);

        return sb.toString();
    }

}
