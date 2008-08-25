package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;

import java.util.*;

/** Hashmap which distributes its keys and values across the cluster. A PUT/GET/REMOVE computes the cluster node to which
 * or from which to get/set the key/value from a hash of the key and then forwards the request to the remote cluster node.
 * We also maintain a local cache (L1 cache) which is a bounded cache that caches retrieved keys/values.
 * @author Bela Ban
 * @version $Id: PartitionedHashMap.java,v 1.4 2008/08/25 15:06:46 belaban Exp $
 */
@Experimental @Unsupported
public class PartitionedHashMap<K,V> implements MembershipListener {

    /** The cache in which all partitioned entries are located */
    private Cache<K,V> l2_cache=new Cache<K,V>();

    /** The local bounded cache, to speed up access to frequently accessed entries. Can be disabled or enabled */
    private Cache<K,V> l1_cache=null;

    private static final Log log=LogFactory.getLog(PartitionedHashMap.class);
    private JChannel ch=null;
    private Address local_addr=null;
    private RpcDispatcher disp=null;
    private String props="udp.xml";
    private String cluster_name="PartitionedHashMap-Cluster";
    private long call_timeout=1000L;
    private long caching_time=30000L; // in milliseconds. -1 means don't cache, 0 means cache forever (or until changed)
    private HashFunction<K> hash_function=null;
    private Set<MembershipListener> membership_listeners=new HashSet<MembershipListener>();

    /** On a view change, if a member P1 detects that for any given key K, P1 is not the owner of K, then
     * it will compute the new owner P2 and transfer ownership for all Ks for which P2 is the new owner. P1
     * will then also evict those keys from its L2 cache */
    private boolean migrate_data=false;


    public interface HashFunction<K> {
        /**
         * Defines a hash function to pick the right node from the list of cluster nodes. Ideally, this function uses
         * consistent hashing, so that the same key maps to the same node despite cluster view changes. If a view change
         * causes all keys to hash to different nodes, then PartitionedHashMap will redirect requests to different nodes
         * and this causes unnecessary overhead.
         * @param key The object to be hashed
         * @return
         */
        Address hash(K key);
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

    public Address getLocalAddress() {
        return local_addr;
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

    public boolean isMigrateData() {
        return migrate_data;
    }

    public void setMigrateData(boolean migrate_data) {
        this.migrate_data=migrate_data;
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

    public Cache<K,V> getL1Cache() {
        return l1_cache;
    }

    public void setL1Cache(Cache<K,V> cache) {
        if(l1_cache != null)
            l1_cache.stop();
        l1_cache=cache;
    }

    public Cache<K,V> getL2Cache() {
        return l2_cache;
    }

    public void setL2Cache(Cache<K,V> cache) {
        if(l2_cache != null)
            l2_cache.stop();
        l2_cache=cache;
    }



    public void start() throws Exception {
        hash_function=new ConsistentHashFunction();
        addMembershipListener((MembershipListener)hash_function);
        ch=new JChannel(props);
        disp=new RpcDispatcher(ch, null, this, this);
        ch.connect(cluster_name);
        local_addr=ch.getLocalAddress();
    }


    public void stop() {
        if(l1_cache != null)
            l1_cache.stop();
        l2_cache.stop();
        disp.stop();
        ch.close();
    }


    public void put(K key, V val) {
        put(key, val, caching_time);
    }

    /**
     * Adds a key/value to the cache, replacing a previous item if there was one
     * @param key The key
     * @param val The value
     * @param caching_time Time to live. -1 means never cache, 0 means cache forever. All other (positive) values
     * are the number of milliseconds to cache the item
     */
    public void put(K key, V val, long caching_time) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_put",
                                  new Object[]{key, val, caching_time},
                                  new Class[]{key.getClass(), val.getClass(), long.class},
                                  GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_put() failed", t);
        }
    }

    public V get(K key) {
        Address dest_node=getNode(key);

        if(l1_cache != null) {
            V val=l1_cache.get(key);
            if(val != null) {
                if(log.isTraceEnabled())
                    log.trace("returned value " + val + " for " + key + " from L1 cache");
                return val;
            }
        }

        try {
            Cache.Value<V> val=(Cache.Value<V>)disp.callRemoteMethod(dest_node, "_get",
                                                                     new Object[]{key},
                                                                     new Class[]{key.getClass()},
                                                                     GroupRequest.GET_FIRST, call_timeout);
            if(val != null) {
                V retval=val.getValue();
                if(l1_cache != null && val.getExpirationTime() >= 0)
                    l1_cache.put(key, retval, val.getExpirationTime());
                return retval;
            }
            return null;
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_get() failed", t);
            return null;
        }
    }


    public void remove(K key) {
        Address dest_node=getNode(key);
        try {
            disp.callRemoteMethod(dest_node, "_remove",
                                  new Object[]{key},
                                  new Class[]{key.getClass()},
                                  GroupRequest.GET_NONE, 0);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_remove() failed", t);
        }
    }
    

    public void _put(K key, V val, long caching_time) {
        if(log.isTraceEnabled())
            log.trace("_put(" + key + ", " + val + ", " + caching_time + ")");
        l2_cache.put(key, val, caching_time);
    }

    public Cache.Value<V> _get(K key) {
        if(log.isTraceEnabled())
            log.trace("_get(" + key + ")");
        return l2_cache.getEntry(key);
    }

    public void _remove(K key) {
        if(log.isTraceEnabled())
            log.trace("_remove(" + key + ")");
        l2_cache.remove(key);
    }





    public void viewAccepted(View new_view) {
        System.out.println("view = " + new_view);
        for(MembershipListener l: membership_listeners) {
            l.viewAccepted(new_view);
        }

        if(migrate_data) {
            migrateData();
        }
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    private void migrateData() {
        for(Map.Entry<K,Cache.Value<V>> entry: l2_cache.entrySet()) {
            K key=entry.getKey();
            Address node=getNode(key);
            if(!node.equals(local_addr)) {
                Cache.Value<V> val=entry.getValue();
                put(key, val.getValue(), val.getExpirationTime());
                l2_cache.remove(key);
                if(log.isTraceEnabled())
                    log.trace("migrated " + key + " from " + local_addr + " to " + node);
            }
        }
    }

    private Address getNode(K key) {
        return hash_function.hash(key);
    }


    private static class ConsistentHashFunction implements HashFunction, MembershipListener {
        private SortedMap<Short,Address> nodes=new  TreeMap<Short,Address>();
        private final static int HASH_SPACE=2000; // must be > max number of nodes in a cluster

        public Address hash(Object key) {
            int hash=Math.abs(key.hashCode());
            int index=hash % HASH_SPACE;

            Address retval;
            for(int i=index; i < index + HASH_SPACE; i++) {
                short new_index=(short)(i % HASH_SPACE);
                retval=nodes.get(new_index);
                if(retval != null) {
                    if(log.isTraceEnabled())
                        log.trace("index for " + key + " is " + index + ", node is " + retval);
                    return retval;
                }
            }
            return null;
        }

        public void viewAccepted(View new_view) {
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

            if(log.isTraceEnabled()) {
                StringBuilder sb=new StringBuilder("node mappings:\n");
                for(Map.Entry<Short,Address> entry: nodes.entrySet()) {
                    sb.append(entry.getKey() + ": " + entry.getValue()).append("\n");
                }
                log.trace(sb);
            }
        }

        public void suspect(Address suspected_mbr) {
        }

        public void block() {
        }
    }
}
