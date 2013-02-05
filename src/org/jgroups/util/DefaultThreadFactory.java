package org.jgroups.util;

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
    protected final String  baseName;
    protected final boolean createDaemons;
    protected final boolean use_numbering;
    protected short         counter=0; // if numbering is enabled
    protected boolean       includeClusterName=false;
    protected String        clusterName=null;
    protected boolean       includeLocalAddress=false;
    protected String        address=null;



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


    public Thread newThread(Runnable r, String name) {
        return newThread(r, name, null, null);
    }

    public Thread newThread(Runnable r) {
        return newThread(r, baseName, null, null);
    }

    public Thread newThread(ThreadGroup group, Runnable r, String name) {
        return newThread(r, name, null, null);
    }

    protected Thread newThread(Runnable r,
                               String name,
                               String addr,
                               String cluster_name) {
        Thread retval=new Thread(r, name);
        retval.setDaemon(createDaemons);
        renameThread(retval, addr, cluster_name);
        return retval;
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

        if(cluster_name == null)
            cluster_name=clusterName;
        if(addr == null)
            addr=this.address;

        if(!includeClusterName && !includeLocalAddress && cluster_name != null) {
            sb.append(",shared=").append(cluster_name);
            thread.setName(sb.toString());
            return;
        }

        if(includeClusterName)
            sb.append(',').append(cluster_name);

        if(includeLocalAddress)
            sb.append(',').append(addr);

        if(use_numbering || includeClusterName || includeLocalAddress)
            thread.setName(sb.toString());
    }

    protected void renameThread(Thread thread, String addr, String cluster_name) {
        renameThread(null, thread, addr, cluster_name);
    }

    public void renameThread(Thread thread) {
        renameThread(null, thread);
    }
}
