package org.jgroups.util;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Lazily names threads: whenever the address or cluster name is changed, all threads are renamed
 * @author Bela Ban
 */
public class LazyThreadFactory extends DefaultThreadFactory {
    private final Collection<WeakReference<Thread>> threads=new ConcurrentLinkedQueue<>();
    private static final String                     ADDR="<ADDR>";
    private static final String                     CLUSTER="<CLUSTER>";


    public LazyThreadFactory(String baseName, boolean createDaemons, boolean use_numbering) {
        super(baseName, createDaemons, use_numbering);
    }

    public Thread newThread(Runnable r) {
         return newThread(r, baseName);
     }

    public Thread newThread(Runnable r, String name) {
        Thread retval=null;
        String addr=address;
        if(addr == null)
            addr=ADDR;
        String cluster_name=clusterName;
        if(cluster_name == null)
            cluster_name=CLUSTER;

        retval=super.newThread(r, name, addr, cluster_name);
        int size=threads.size();
        threads.add(new WeakReference<>(retval));
        if(size > 0)
            removeTerminatedThreads();
        return retval;
    }


    public void setAddress(String address) {
        boolean changed=false;
        if(!Util.match(this.address, address))
            changed=true;
        super.setAddress(address);
        if(changed)
            renameThreads();
    }

    public void setClusterName(String cluster_name) {
        boolean changed=false;
        if(!Util.match(this.clusterName, cluster_name))
            changed=true;
        super.setClusterName(cluster_name);
        if(changed)
            renameThreads();
    }

    public int size() {return threads.size();}

    public void destroy() {
        threads.clear();
    }

    public void removeTerminatedThreads() {
        for(Iterator<WeakReference<Thread>> it=threads.iterator(); it.hasNext();) {
            WeakReference<Thread> ref=it.next();
            Thread thread=ref.get();
            if(thread == null || thread.getState() == Thread.State.TERMINATED)
                it.remove();
        }
    }

    public String dumpThreads() {
        StringBuilder sb=new StringBuilder();
        int cnt=1;
        for(WeakReference<Thread> ref: threads)
            sb.append(String.format("ref %d: %s (%s)\n", cnt++, ref.get(), ref.get().getState()));
        return sb.toString();
    }

    protected void renameThreads() {
        for(Iterator<WeakReference<Thread>> it=threads.iterator(); it.hasNext();) {
            WeakReference<Thread> ref=it.next();
            Thread thread=ref.get();
            if(thread == null || thread.getState() == Thread.State.TERMINATED) {
                it.remove();
                continue;
            }
            String name=thread.getName();
            name=changeName(name);
            thread.setName(name);
            if(!name.contains(ADDR) && !name.contains(CLUSTER))
                it.remove();
        }
    }




    /** Replaces "<ADDR>" with the local address and <CLUSTER> with the cluster name */
    protected String changeName(String name) {
        String retval=name;
        StringBuilder tmp;

        if(address != null) {
            tmp=new StringBuilder(address);
            retval=retval.replace(ADDR, tmp);
        }
        if(clusterName != null) {
            tmp=new StringBuilder(clusterName);
            retval=retval.replace(CLUSTER, tmp);
        }
        return retval;
    }
}
