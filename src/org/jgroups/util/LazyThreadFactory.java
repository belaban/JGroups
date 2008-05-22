package org.jgroups.util;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

/**
 * Lazily names threads: whenever the address or cluster name is changed, all threads are renamed
 * @author Bela Ban
 * @version $Id: LazyThreadFactory.java,v 1.1.2.2 2008/05/22 13:28:37 belaban Exp $
 */
public class LazyThreadFactory extends DefaultThreadFactory {
    private List<WeakReference<Thread>> threads=new LinkedList<WeakReference<Thread>>();
    private static final StringBuilder ADDR=new StringBuilder("<ADDR>");
    private static final StringBuilder CL_NAME=new StringBuilder("<CL-NAME>");

    public LazyThreadFactory(ThreadGroup group, String baseName, boolean createDaemons) {
        super(group, baseName, createDaemons);
    }

    public LazyThreadFactory(ThreadGroup group, String baseName, boolean createDaemons, boolean use_numbering) {
        super(group, baseName, createDaemons, use_numbering);
    }

    public Thread newThread(ThreadGroup group, Runnable r, String name) {
        Thread retval=null;
        String addr=address;
        if(addr == null)
            addr="<ADDR>";
        String cluster_name=clusterName;
        if(cluster_name == null)
            cluster_name="<CL-NAME>";

        retval=super.newThread(group, r, name, addr, cluster_name);
        threads.add(new WeakReference<Thread>(retval));
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

    protected void renameThreads() {
        for(Iterator<WeakReference<Thread>> it=threads.iterator(); it.hasNext();) {
            WeakReference<Thread> ref=it.next();
            Thread thread=ref.get();
            if(thread == null) {
                it.remove();
                continue;
            }
            String name=thread.getName();
            name=changeName(name);
            thread.setName(name);
        }
    }

    /** Replaces "<ADDR>" with the address and <CL-NAME> with cluster name */
    private String changeName(String name) {
        String retval=name;
        StringBuilder tmp;

        if(address != null) {
            tmp=new StringBuilder(address);
            retval=retval.replace(ADDR, tmp);
        }
        if(clusterName != null) {
            tmp=new StringBuilder(clusterName);
            retval=retval.replace(CL_NAME, tmp);
        }

        return retval;
    }
}
