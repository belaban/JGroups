package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Unsupported;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

/** Hashmap which distributes its keys and values across the cluster. A PUT/GET/REMOVE computes the cluster node to which
 * or from which to get/set the key/value from a hash of the key and then forwards the request to the remote cluster node.
 * We also maintain a local cache (L1 cache) which is a bounded cache that caches retrieved keys/values. <br/>
 * Todos:<br/>
 * <ol>
 * <li>Use MarshalledValue to keep track of byte[] buffers, and be able to compute the exact size of the cache. This is
 *     good for maintaining a bounded cache (rather than using the number of entries)
 * <li>Provide a better consistent hashing algorithm than ConsistentHashFunction as default
 * <li>GUI (showing at least the topology and L1 and L2 caches)
 * <li>Notifications (puts, removes, gets etc)
 * <li>Invalidation of L1 caches (if used) on removal/put of item
 * <li>Benchmarks, comparison to memcached
 * <li>Documentation, comparison to memcached
 * </ol>
 * @author Bela Ban
 */
@Experimental @Unsupported
public class PartitionedHashMap<K,V> implements Receiver {

    /** The cache in which all partitioned entries are located */
    private Cache<K,V> l2_cache=new Cache<>();

    /** The local bounded cache, to speed up access to frequently accessed entries. Can be disabled or enabled */
    private Cache<K,V> l1_cache=null;

    private static final Log log=LogFactory.getLog(PartitionedHashMap.class);
    private JChannel ch=null;
    private Address local_addr=null;
    private View    view;
    private RpcDispatcher disp=null;
    @ManagedAttribute(writable=true)
    private String props="udp.xml";
    @ManagedAttribute(writable=true)
    private String cluster_name="PartitionedHashMap-Cluster";
    @ManagedAttribute(writable=true)
    private long call_timeout=1000L;
    @ManagedAttribute(writable=true)
    private long caching_time=30000L; // in milliseconds. -1 means don't cache, 0 means cache forever (or until changed)
    private HashFunction<K> hash_function=null;
    private final Set<Receiver> membership_listeners=new HashSet<>();

    /** On a view change, if a member P1 detects that for any given key K, P1 is not the owner of K, then
     * it will compute the new owner P2 and transfer ownership for all Ks for which P2 is the new owner. P1
     * will then also evict those keys from its L2 cache */
    @ManagedAttribute(writable=true)
    private boolean migrate_data=false;

    private static final short PUT     = 1;
    private static final short GET     = 2;
    private static final short REMOVE  = 3;

    protected static final Map<Short,Method> methods=Util.createConcurrentMap(8);

     static {
        try {
            methods.put(PUT, PartitionedHashMap.class.getMethod("_put",
                                                                Object.class,
                                                                Object.class,
                                                                long.class));
            methods.put(GET, PartitionedHashMap.class.getMethod("_get",
                                                               Object.class));
            methods.put(REMOVE, PartitionedHashMap.class.getMethod("_remove", Object.class));
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public interface HashFunction<K> {
        /**
         * Defines a hash function to pick the right node from the list of cluster nodes. Ideally, this function uses
         * consistent hashing, so that the same key maps to the same node despite cluster view changes. If a view change
         * causes all keys to hash to different nodes, then PartitionedHashMap will redirect requests to different nodes
         * and this causes unnecessary overhead.
         * @param key The object to be hashed
         * @param membership The membership. This value can be ignored for example if the hash function keeps
         * track of the membership itself, e.g. by registering as a membership
         * listener ({@link PartitionedHashMap#addMembershipListener(Receiver)} )
         * @return
         */
        Address hash(K key, List<Address> membership);
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

    @ManagedAttribute
    public String getLocalAddressAsString() {
        return local_addr != null? local_addr.toString() : "null";
    }

    @ManagedAttribute
    public String getView() {
        return view != null? view.toString() : "null";
    }

    @ManagedAttribute
    public boolean isL1CacheEnabled() {
        return l1_cache != null;
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

    public void setHashFunction(HashFunction<K> hash_function) {
        this.hash_function=hash_function;
    }

    public void addMembershipListener(Receiver r) {
        membership_listeners.add(r);
    }

    public void removeMembershipListener(Receiver r) {
        membership_listeners.remove(r);
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


    @ManagedOperation
    public void start() throws Exception {
        hash_function=new ConsistentHashFunction<>();
        addMembershipListener((Receiver)hash_function);
        ch=new JChannel(props);
        Marshaller m=new CustomMarshaller();
        disp=new RpcDispatcher(ch, this).setMarshaller(m).setMethodLookup(methods::get).setReceiver(this);
        ch.connect(cluster_name);
        local_addr=ch.getAddress();
        view=ch.getView();
    }

    @ManagedOperation
    public void stop() {
        if(l1_cache != null)
            l1_cache.stop();
        if(migrate_data) {
            List<Address> members_without_me=new ArrayList<>(view.getMembers());
            members_without_me.remove(local_addr);

            for(Map.Entry<K,Cache.Value<V>> entry: l2_cache.entrySet()) {
                K key=entry.getKey();
                Address node=hash_function.hash(key, members_without_me);
                if(!node.equals(local_addr)) {
                    Cache.Value<V> val=entry.getValue();
                    sendPut(node, key, val.getValue(), val.getTimeout(), true);
                    if(log.isTraceEnabled())
                        log.trace("migrated " + key + " from " + local_addr + " to " + node);
                }
            }
        }
        l2_cache.stop();
        disp.stop();
        ch.close();
    }

    @ManagedOperation
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
    @ManagedOperation
    public void put(K key, V val, long caching_time) {
        Address dest_node=getNode(key);
        if(dest_node.equals(local_addr)) {
            l2_cache.put(key, val, caching_time);
        }
        else {
            sendPut(dest_node, key, val, caching_time, false);
        }
        if(l1_cache != null && caching_time >= 0)
            l1_cache.put(key, val, caching_time);
    }

    @ManagedOperation
    public V get(K key) {
        if(l1_cache != null) {
            V val=l1_cache.get(key);
            if(val != null) {
                if(log.isTraceEnabled())
                    log.trace("returned value " + val + " for " + key + " from L1 cache");
                return val;
            }
        }

        Cache.Value<V> val;
        try {
            Address dest_node=getNode(key);
            // if we are the destination, don't invoke an RPC but return the item from our L2 cache directly !
            if(dest_node.equals(local_addr)) {
                val=l2_cache.getEntry(key);
            }
            else {
                val=disp.callRemoteMethod(dest_node, new MethodCall(GET, key),
                                          new RequestOptions(ResponseMode.GET_FIRST, call_timeout));
            }
            if(val != null) {
                V retval=val.getValue();
                if(l1_cache != null && val.getTimeout() >= 0)
                    l1_cache.put(key, retval, val.getTimeout());
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

    @ManagedOperation
    public void remove(K key) {
        Address dest_node=getNode(key);

        try {
            if(dest_node.equals(local_addr)) {
                l2_cache.remove(key);
            }
            else {
                disp.callRemoteMethod(dest_node, new MethodCall(REMOVE, key), new RequestOptions(ResponseMode.GET_NONE, call_timeout));
            }
            if(l1_cache != null)
                l1_cache.remove(key);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_remove() failed", t);
        }
    }
    

    public V _put(K key, V val, long caching_time) {
        if(log.isTraceEnabled())
            log.trace("_put(" + key + ", " + val + ", " + caching_time + ")");
        return l2_cache.put(key, val, caching_time);
    }

    public Cache.Value<V> _get(K key) {
        if(log.isTraceEnabled())
            log.trace("_get(" + key + ")");
        return l2_cache.getEntry(key);
    }

    public V _remove(K key) {
        if(log.isTraceEnabled())
            log.trace("_remove(" + key + ")");
        return l2_cache.remove(key);
    }





    public void viewAccepted(View new_view) {
        System.out.println("view = " + new_view);
        this.view=new_view;
        for(Receiver r: membership_listeners)
            r.viewAccepted(new_view);
        if(migrate_data)
            migrateData();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        if(l1_cache != null)
            sb.append("L1 cache: " + l1_cache.getSize() + " entries");
        sb.append("\nL2 cache: " + l2_cache.getSize() + "entries()");
        return sb.toString();
    }


    @ManagedOperation
    public String dump() {
        StringBuilder sb=new StringBuilder();
        if(l1_cache != null) {
            sb.append("L1 cache:\n").append(l1_cache.dump());
        }
        sb.append("\nL2 cache:\n").append(l2_cache.dump());

        return sb.toString();
    }


    private void migrateData() {
        for(Map.Entry<K,Cache.Value<V>> entry: l2_cache.entrySet()) {
            K key=entry.getKey();
            Address node=getNode(key);
            if(!node.equals(local_addr)) {
                Cache.Value<V> val=entry.getValue();
                put(key, val.getValue(), val.getTimeout());
                l2_cache.remove(key);
                if(log.isTraceEnabled())
                    log.trace("migrated " + key + " from " + local_addr + " to " + node);
            }
        }
    }

    private void sendPut(Address dest, K key, V val, long caching_time, boolean synchronous) {
        try {
            ResponseMode mode=synchronous? ResponseMode.GET_ALL : ResponseMode.GET_NONE;
            disp.callRemoteMethod(dest, new MethodCall(PUT, key, val, caching_time),
                                  new RequestOptions(mode, call_timeout));
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("_put() failed", t);
        }
    }

    private Address getNode(K key) {
        return hash_function.hash(key, null);
    }


    public static class ConsistentHashFunction<K> implements Receiver, HashFunction<K> {
        private final SortedMap<Short,Address> nodes=new TreeMap<>();
        private final static int HASH_SPACE=2048; // must be > max number of nodes in a cluster, and a power of 2

        public Address hash(K key, List<Address> members) {
            int index=Math.abs(key.hashCode() & (HASH_SPACE - 1));
            if(members != null && !members.isEmpty()) {
                SortedMap<Short,Address> tmp=new TreeMap<>(nodes);
                tmp.entrySet().removeIf(entry -> !members.contains(entry.getValue()));
                return findFirst(tmp, index);
            }
            return findFirst(nodes, index);
        }

        public void viewAccepted(View new_view) {
            nodes.clear();
            for(Address node: new_view.getMembers()) {
                int hash=Math.abs(node.hashCode() & (HASH_SPACE - 1));
                for(int i=hash; i < hash + HASH_SPACE; i++) {
                    short new_index=(short)(i & (HASH_SPACE - 1));
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

        private static Address findFirst(Map<Short,Address> map, int index) {
            Address retval;
            for(int i=index; i < index + HASH_SPACE; i++) {
                short new_index=(short)(i & (HASH_SPACE - 1));
                retval=map.get(new_index);
                if(retval != null)
                    return retval;
            }
            return null;
        }
    }





    private static class CustomMarshaller implements Marshaller {
        static final byte NULL        = 1;
        static final byte OBJ         = 2;
        static final byte VALUE       = 3;

        @Override
        public void objectToStream(Object obj, DataOutput out) throws IOException {
            if(obj == null) {
                out.write(NULL);
                return;
            }
            if(obj instanceof Cache.Value) {
                Cache.Value value=(Cache.Value)obj;
                out.writeByte(VALUE);
                out.writeLong(value.getTimeout());
                Util.objectToStream(value.getValue(), out);
            }
            else {
                out.writeByte(OBJ);
                Util.objectToStream(obj, out);
            }
        }

        @Override
        public Object objectFromStream(DataInput in) throws IOException, ClassNotFoundException {
            byte type=in.readByte();
            if(type == NULL)
                return null;
            if(type == VALUE) {
                long expiration_time=in.readLong();
                Object obj=Util.objectFromStream(in);
                return new Cache.Value(obj, expiration_time);
            }
            return Util.objectFromStream(in);
        }
    }
}
