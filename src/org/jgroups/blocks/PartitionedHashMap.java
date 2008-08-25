package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;

/** Hashmap which distributes its keys and values across the cluster. A PUT/GET/REMOVE computes the cluster node to which
 * or from which to get/set the key/value from a hash of the key and then forwards the request to the remote cluster node.
 * We also maintain a local cache (L1 cache) which is a bounded cache that caches retrieved keys/values.
 * @author Bela Ban
 * @version $Id: PartitionedHashMap.java,v 1.1 2008/08/25 07:23:01 belaban Exp $
 */
@Experimental @Unsupported
public class PartitionedHashMap implements MembershipListener {
    private final ConcurrentMap<Object,Object> cache=new ConcurrentHashMap<Object,Object>();
    private View view=null;
    private static final Log log=LogFactory.getLog(PartitionedHashMap.class);
    private JChannel ch=null;
    private Address local_addr=null;
    private RpcDispatcher disp=null;
    private String props="udp.xml";
    private String cluster_name="PartitionedHashMap-Cluster";
    private Address[] nodes=null;
    private long call_timeout=1000L;
    private HashFunction hash_function=null;


    public interface HashFunction {
        /**
         * Defines a hash function to pick the right node from the list of cluster nodes. Ideally, this function uses
         * consistent hashing, so that the same key maps to the same node despite cluster view changes. If a view change
         * causes all keys to hash to different nodes, then PartitionedHashMap will redirect requests to different nodes
         * and this causes unnecessary overhead.
         * @param nodes
         * @return
         */
        Address hash(List<Address> nodes);
    }


    public PartitionedHashMap(String props, String cluster_name) {
        this.props=props;
        this.cluster_name=cluster_name;
    }

    public String getProps() {
        return props;
    }

    public void setProps(String props) {
        this.props=props;
    }

    public String getClusterName() {
        return cluster_name;
    }

    public void setClusterName(String cluster_name) {
        this.cluster_name=cluster_name;
    }

    public HashFunction getHashFunction() {
        return hash_function;
    }

    public void setHashFunction(HashFunction hash_function) {
        this.hash_function=hash_function;
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        disp=new RpcDispatcher(ch, null, this, this);
        ch.connect(cluster_name);
        local_addr=ch.getLocalAddress();
    }


    public void stop() {
        disp.stop();
        ch.close();
    }


    public void put(Object key, Object val) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_put",
                                  new Object[]{key, val},
                                  new Class[]{Object.class, Object.class},
                                  GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_put() failed", t);
        }
    }

    /**
     * Adds a key/value to the cache, replacing a previous item if there was one
     * @param key The key
     * @param val The value
     * @param caching_time Time to live. -1 means never cache, 0 means cache forever. All other (positive) values
     * are the number of milliseconds to cache the item
     */
    public void put(Object key, Object val, int caching_time) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_put",
                                  new Object[]{key, val, caching_time},
                                  new Class[]{Object.class, Object.class, int.class},
                                  GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_put() failed", t);
        }
    }

    public Object get(Object key) {
        Address dest_node=getNode(key);
        try {
            return disp.callRemoteMethod(dest_node, "_get",
                                                new Object[]{key},
                                                new Class[]{Object.class},
                                                GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_get() failed", t);
            return null;
        }
    }

    public void remove(Object key) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_remove",
                                  new Object[]{key},
                                  new Class[]{Object.class},
                                  GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_remove() failed", t);
        }
    }
    


    public void _put(Object key, Object val) {
        if(log.isTraceEnabled())
            log.trace("_put(" + key + ", " + val + ")");
        cache.put(key, val);
    }

    public void _put(Object key, Object val, int caching_time) {
        if(log.isTraceEnabled())
            log.trace("_put(" + key + ", " + val + ", " + caching_time + ")");
        cache.put(key, val);
    }

    public Object _get(Object key) {
        if(log.isTraceEnabled())
            log.trace("_get(" + key + ")");
        return cache.get(key);
    }

    public void _remove(Object key) {
        if(log.isTraceEnabled())
            log.trace("_remove(" + key + ")");
        cache.remove(key);
    }





    public void viewAccepted(View new_view) {

        System.out.println("new_view = " + new_view);
        this.view=new_view;
        nodes=new Address[new_view.size()];
        int index=0;
        for(Address addr: new_view.getMembers()) {
            nodes[index++]=addr;
        }
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    private Address getNode(Object key) {
        if(hash_function != null)
            return hash_function.hash(view.getMembers());

        int hash=Math.abs(key.hashCode());
        int index=hash % nodes.length;
        return nodes[index];
    }
}
