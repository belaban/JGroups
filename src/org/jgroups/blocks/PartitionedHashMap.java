package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.*;

/** Hashmap which distributes its keys and values across the cluster. A PUT/GET/REMOVE computes the cluster node to which
 * or from which to get/set the key/value from a hash of the key and then forwards the request to the remote cluster node.
 * We also maintain a local cache (L1 cache) which is a bounded cache that caches retrieved keys/values.
 * @author Bela Ban
 * @version $Id: PartitionedHashMap.java,v 1.2 2008/08/25 12:01:32 belaban Exp $
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
    private long call_timeout=1000L;
    private long caching_time=30000L; // in milliseconds. -1 means don't cache, 0 means cache forever (or until changed)
    private HashFunction hash_function=null;
    private Set<MembershipListener> membership_listeners=new HashSet<MembershipListener>();


    public interface HashFunction {
        /**
         * Defines a hash function to pick the right node from the list of cluster nodes. Ideally, this function uses
         * consistent hashing, so that the same key maps to the same node despite cluster view changes. If a view change
         * causes all keys to hash to different nodes, then PartitionedHashMap will redirect requests to different nodes
         * and this causes unnecessary overhead.
         * @param key The object to be hashed
         * @return
         */
        Address hash(Object key);
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

    public long getCallTimeout() {
        return call_timeout;
    }

    public void setCallTimeout(long call_timeout) {
        this.call_timeout=call_timeout;
    }

    public long getCachingTime() {
        return caching_time;
    }

    public void setCachingTime(long caching_time) {
        this.caching_time=caching_time;
    }

    public HashFunction getHashFunction() {
        return hash_function;
    }

    public void setHashFunction(HashFunction hash_function) {
        this.hash_function=hash_function;
    }

    public void addMembershipListener(MembershipListener l) {
        membership_listeners.add(l);
    }

    public void removeMembershipListener(MembershipListener l) {
        membership_listeners.remove(l);
    }

    public void start() throws ChannelException {
        hash_function=new ConsistentHashFunction();
        addMembershipListener((MembershipListener)hash_function);
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
        put(key, val, this.caching_time);
    }

    /**
     * Adds a key/value to the cache, replacing a previous item if there was one
     * @param key The key
     * @param val The value
     * @param caching_time Time to live. -1 means never cache, 0 means cache forever. All other (positive) values
     * are the number of milliseconds to cache the item
     */
    public void put(Object key, Object val, long caching_time) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_put",
                                  new Object[]{key, val, caching_time},
                                  new Class[]{Object.class, Object.class, long.class},
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
                                                GroupRequest.GET_FIRST, call_timeout);
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

    public void _put(Object key, Object val, long caching_time) {
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
        for(MembershipListener l: membership_listeners) {
            l.viewAccepted(new_view);
        }
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    private Address getNode(Object key) {
        return hash_function.hash(key);
    }


    private static class ConsistentHashFunction implements HashFunction, MembershipListener {
        private SortedMap<Short,Address> nodes=new  TreeMap<Short,Address>();
        private final static int HASH_SPACE=2000; // must be > max number of nodes in a cluster

        public Address hash(Object key) {
            int hash=Math.abs(key.hashCode());
            int index=hash % HASH_SPACE;
            System.out.println("hash for " + key + " is " + index);
            Address retval;
            for(int i=index; i < index + HASH_SPACE; i++) {
                short new_index=(short)(i % HASH_SPACE);
                retval=nodes.get(new_index);
                if(retval != null) {
                    System.out.println("hashed " + key + " to " + retval);
                    return retval;
                }
            }
            return null;
        }

        public void viewAccepted(View new_view) {
            System.out.println("nodes:");
            nodes.clear();
            for(Address node: new_view.getMembers()) {
                int hash=Math.abs(node.hashCode()) % HASH_SPACE;
                for(int i=hash; i < hash + HASH_SPACE; i++) {
                    short new_index=(short)(i % HASH_SPACE);
                    if(!nodes.containsKey(new_index)) {
                        nodes.put(new_index, node);
                        break;
                    }
                }
            }

            for(Map.Entry<Short,Address> entry: nodes.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }

        public void suspect(Address suspected_mbr) {
        }

        public void block() {
        }
    }
}
