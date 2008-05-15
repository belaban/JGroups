package org.jgroups.util;

/**
 * Thread factory mainly responsible for naming of threads. Can be replaced by user. If use_numbering is set, a thread
 * THREAD will be called THREAD-1, THREAD-2, and so on.<p/>
 * If a pattern has been set (through setPattern()), then the cluster name and local address will also be added, e.g.
 * THREAD-5,MyCluster,192.168.1.5:63754 or THREAD,MyCluster,192.168.1.5:63754
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @version $Id: DefaultThreadFactory.java,v 1.2 2008/05/15 14:14:15 belaban Exp $
 */
public class DefaultThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final String      baseName;
    private final boolean     createDaemons;
    private short             counter=0; // if numbering is enabled
    private final boolean     use_numbering;

    private boolean           includeClusterName=false;
    private boolean           includeLocalAddress=false;
    private String            clusterName=null;
    private String            address=null;


    public DefaultThreadFactory(ThreadGroup group, String baseName, boolean createDaemons) {
        this(group, baseName, createDaemons, false);
    }

    public DefaultThreadFactory(ThreadGroup group, String baseName, boolean createDaemons, boolean use_numbering) {
        this.group = group;
        this.baseName = baseName;
        this.createDaemons = createDaemons;
        this.use_numbering=use_numbering;
    }

    public void setPattern(String pattern) {
        includeClusterName=pattern.contains("c");
        includeLocalAddress=pattern.contains("l");
    }

    public void setClusterName(String channelName){
        clusterName=channelName;
    }

    public void setAddress(String address){
        this.address=address;
    }

    public Thread newThread(Runnable r, String name) {
        return newThread(group, r, name);
    }

    public Thread newThread(Runnable r) {
        return newThread(group, r, baseName);
    }

    public Thread newThread(ThreadGroup group, Runnable r, String name) {
        Thread retval=new Thread(group, r, name);
        retval.setDaemon(createDaemons);
        renameThread(retval);
        return retval;
    }


    protected void renameThread(Thread thread) {
        if(thread == null) return;
        StringBuilder sb=new StringBuilder(thread.getName());
        if(use_numbering) {
            short id;
            synchronized(this) {
                id=++counter;
            }
            sb.append("-" + id);
        }
        if(includeClusterName)
            sb.append(',').append(clusterName);

        if(includeLocalAddress)
            sb.append(',').append(address);

        if(use_numbering || includeClusterName || includeLocalAddress)
            thread.setName(sb.toString());
    }



}