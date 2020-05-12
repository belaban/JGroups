package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.*;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Cache which allows for replication factors <em>per data items</em>; the factor determines how many replicas
 * of a key/value we create across the cluster.<br/>
 * See doc/design/ReplCache.txt for details.
 * @author Bela Ban
 */
public class ReplCache<K,V> implements Receiver, Cache.ChangeListener {

    /** The cache in which all entries are located. The value is a tuple, consisting of the replication count and the
     * actual value */
    private Cache<K,Value<V>> l2_cache=new Cache<>();

    /** The local bounded cache, to speed up access to frequently accessed entries. Can be disabled or enabled */
    private Cache<K,V> l1_cache=null;

    private static final Log log=LogFactory.getLog(ReplCache.class);
    private JChannel ch=null;
    private Address local_addr;
    private View    view;
    private RpcDispatcher disp;
    @ManagedAttribute(writable=true)
    private String props="udp.xml";
    @ManagedAttribute(writable=true)
    private String cluster_name="ReplCache-Cluster";
    @ManagedAttribute(writable=true)
    private long call_timeout=1000L;
    @ManagedAttribute(writable=true)
    private long caching_time=30000L; // in milliseconds. -1 means don't cache, 0 means cache forever (or until changed)

    @ManagedAttribute
    private short default_replication_count=1; // no replication by default

    private HashFunction<K> hash_function=null;

    private HashFunctionFactory<K> hash_function_factory=ConsistentHashFunction::new;

    private final Set<Receiver> receivers=new HashSet<>();

    private final Set<ChangeListener> change_listeners=new HashSet<>();

    /** On a view change, if a member P1 detects that for any given key K, P1 is not the owner of K, then
     * it will compute the new owner P2 and transfer ownership for all Ks for which P2 is the new owner. P1
     * will then also evict those keys from its L2 cache */
    @ManagedAttribute(writable=true)
    private boolean migrate_data=true;

    private static final short PUT         = 1;
    private static final short PUT_FORCE   = 2;
    private static final short GET         = 3;
    private static final short REMOVE      = 4;
    private static final short REMOVE_MANY = 5;

    protected static final Map<Short, Method> methods=Util.createConcurrentMap(8);
    private TimeScheduler timer;




    static {
        try {
            methods.put(PUT, ReplCache.class.getMethod("_put",
                                                       Object.class,
                                                       Object.class,
                                                       short.class,
                                                       long.class));
            methods.put(PUT_FORCE, ReplCache.class.getMethod("_put",
                                                             Object.class,
                                                             Object.class,
                                                             short.class,
                                                             long.class, boolean.class));
            methods.put(GET, ReplCache.class.getMethod("_get",
                                                       Object.class));
            methods.put(REMOVE, ReplCache.class.getMethod("_remove", Object.class));
            methods.put(REMOVE_MANY, ReplCache.class.getMethod("_removeMany", Set.class));
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public interface HashFunction<K> {

        /**
         * Function that, given a key and a replication count, returns replication_count number of <em>different</em>
         * addresses of nodes.
         * @param key
         * @param replication_count
         * @return
         */
        List<Address> hash(K key, short replication_count);

        /**
         * When the topology changes, this method will be called. Implementations will typically cache the node list
         * @param nodes
         */
        void installNodes(List<Address> nodes);
    }


    public interface HashFunctionFactory<K> {
        HashFunction<K> create();
    }


    public ReplCache(String props, String cluster_name) {
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
    public int getClusterSize() {
        return view != null? view.size() : 0;
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

    public short getDefaultReplicationCount() {
        return default_replication_count;
    }

    public void setDefaultReplicationCount(short default_replication_count) {
        this.default_replication_count=default_replication_count;
    }

    public HashFunction getHashFunction() {
        return hash_function;
    }

    public void setHashFunction(HashFunction<K> hash_function) {
        this.hash_function=hash_function;
    }

    public HashFunctionFactory getHashFunctionFactory() {
        return hash_function_factory;
    }

    public void setHashFunctionFactory(HashFunctionFactory<K> hash_function_factory) {
        this.hash_function_factory=hash_function_factory;
    }

    public void addReceiver(Receiver r) {
        receivers.add(r);
    }

    public void removeMembershipListener(Receiver r) {
        receivers.remove(r);
    }

    public void addChangeListener(ChangeListener l) {
        change_listeners.add(l);
    }

    public void removeChangeListener(ChangeListener l) {
        change_listeners.remove(l);
    }

    public Cache<K,V> getL1Cache() {
        return l1_cache;
    }

    public void setL1Cache(Cache<K,V> cache) {
        if(l1_cache != null)
            l1_cache.stop();
        l1_cache=cache;
    }

    public Cache<K,Value<V>> getL2Cache() {
        return l2_cache;
    }

    public void setL2Cache(Cache<K,Value<V>> cache) {
        if(cache != null) {
            l2_cache.stop();
            l2_cache=cache;
        }
    }


    @ManagedOperation
    public void start() throws Exception {
        if(hash_function_factory != null) {
            hash_function=hash_function_factory.create();
        }
        if(hash_function == null)
            hash_function=new ConsistentHashFunction<>();

        ch=new JChannel(props);
        disp=new RpcDispatcher(ch, this).setMethodLookup(methods::get).setReceiver(this);
        ch.connect(cluster_name);
        local_addr=ch.getAddress();
        view=ch.getView();
        timer=ch.getProtocolStack().getTransport().getTimer();

        l2_cache.addChangeListener(this);
    }

    @ManagedOperation
    public void stop() {
        if(l1_cache != null)
            l1_cache.stop();
        if(migrate_data) {
            List<Address> members_without_me=new ArrayList<>(view.getMembers());
            members_without_me.remove(local_addr);

            HashFunction<K> tmp_hash_function=hash_function_factory.create();
            tmp_hash_function.installNodes(members_without_me);

            for(Map.Entry<K,Cache.Value<Value<V>>> entry: l2_cache.entrySet()) {
                K key=entry.getKey();
                Cache.Value<Value<V>> val=entry.getValue();
                if(val == null)
                    continue;
                Value<V> tmp=val.getValue();
                if(tmp == null)
                    continue;
                short repl_count=tmp.getReplicationCount();
                if(repl_count != 1) // we only handle keys which are not replicated and which are stored by us
                    continue;

                List<Address> nodes=tmp_hash_function.hash(key, repl_count);
                if(nodes == null || nodes.isEmpty())
                    continue;
                if(!nodes.contains(local_addr)) {
                    Address dest=nodes.get(0); // should only have 1 element anyway
                    move(dest, key, tmp.getVal(), repl_count, val.getTimeout(), true);
                    _remove(key);
                }
            }
        }
        l2_cache.removeChangeListener(this);
        l2_cache.stop();
        disp.stop();
        ch.close();
    }


    /**
     * Places a key/value pair into one or several nodes in the cluster.
     * @param key The key, needs to be serializable
     * @param val The value, needs to be serializable
     * @param repl_count Number of replicas. The total number of times a data item should be present in a cluster.
     * Needs to be &gt; 0
     * <ul>
     * <li>-1: create key/val in all the nodes in the cluster
     * <li>1: create key/val only in one node in the cluster, picked by computing the consistent hash of KEY
     * <li>K &gt; 1: create key/val in those nodes in the cluster which match the consistent hashes created for KEY
     * </ul>
     * @param timeout Expiration time for key/value.
     * <ul>
     * <li>-1: don't cache at all in the L1 cache
     * <li>0: cache forever, until removed or evicted because we need space for newer elements
     * <li>&gt; 0: number of milliseconds to keep an idle element in the cache. An element is idle when not accessed.
     * </ul>
     * @param synchronous Whether or not to block until all cluster nodes have applied the change
     */
    @ManagedOperation
    public void put(K key, V val, short repl_count, long timeout, boolean synchronous) {
        if(repl_count == 0) {
            if(log.isWarnEnabled())
                log.warn("repl_count of 0 is invalid, data will not be stored in the cluster");
            return;
        }
        mcastPut(key, val, repl_count, timeout, synchronous);
        if(l1_cache != null && timeout >= 0)
            l1_cache.put(key, val, timeout);
    }

    
    /**
     * Places a key/value pair into one or several nodes in the cluster.
     * @param key The key, needs to be serializable
     * @param val The value, needs to be serializable
     * @param repl_count Number of replicas. The total number of times a data item should be present in a cluster.
     * Needs to be &gt; 0
     * <ul>
     * <li>-1: create key/val in all the nodes in the cluster
     * <li>1: create key/val only in one node in the cluster, picked by computing the consistent hash of KEY
     * <li>K &gt; 1: create key/val in those nodes in the cluster which match the consistent hashes created for KEY
     * </ul>
     * @param timeout Expiration time for key/value.
     * <ul>
     * <li>-1: don't cache at all in the L1 cache
     * <li>0: cache forever, until removed or evicted because we need space for newer elements
     * <li>&gt; 0: number of milliseconds to keep an idle element in the cache. An element is idle when not accessed.
     * </ul>
     */
    @ManagedOperation
    public void put(K key, V val, short repl_count, long timeout) {
        put(key, val, repl_count, timeout, false); // don't block (asynchronous put) by default
    }



    @ManagedOperation
    public void put(K key, V val) {
        put(key, val, default_replication_count, caching_time);
    }


    
    /**
     * Returns the value associated with key
     * @param key The key, has to be serializable
     * @return The value associated with key, or null
     */
    @ManagedOperation
    public V get(K key) {

        // 1. Try the L1 cache first
        if(l1_cache != null) {
            V val=l1_cache.get(key);
            if(val != null) {
                if(log.isTraceEnabled())
                    log.trace("returned value " + val + " for " + key + " from L1 cache");
                return val;
            }
        }

        // 2. Try the local cache
        Cache.Value<Value<V>> val=l2_cache.getEntry(key);
        Value<V> tmp;
        if(val != null) {
            tmp=val.getValue();
            if(tmp !=null) {
                V real_value=tmp.getVal();
                if(real_value != null && l1_cache != null && val.getTimeout() >= 0)
                    l1_cache.put(key, real_value, val.getTimeout());
                return tmp.getVal();
            }
        }

        // 3. Execute a cluster wide GET
        try {
            RspList<Object> rsps=disp.callRemoteMethods(null,
                                                new MethodCall(GET, key),
                                                new RequestOptions(ResponseMode.GET_ALL, call_timeout));
            for(Rsp rsp: rsps.values()) {
                Object obj=rsp.getValue();
                if(obj == null || obj instanceof Throwable)
                    continue;
                val=(Cache.Value<Value<V>>)rsp.getValue();
                if(val != null) {
                    tmp=val.getValue();
                    if(tmp != null) {
                        V real_value=tmp.getVal();
                        if(real_value != null && l1_cache != null && val.getTimeout() >= 0)
                            l1_cache.put(key, real_value, val.getTimeout());
                        return real_value;
                    }
                }
            }
            return null;
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("get() failed", t);
            return null;
        }
    }

    /**
     * Removes key in all nodes in the cluster, both from their local hashmaps and L1 caches
     * @param key The key, needs to be serializable
     */
    @ManagedOperation
    public void remove(K key) {
        remove(key, false); // by default we use asynchronous removals
    }


    /**
     * Removes key in all nodes in the cluster, both from their local hashmaps and L1 caches
     * @param key The key, needs to be serializable
     */
    @ManagedOperation
    public void remove(K key, boolean synchronous) {
        try {
            disp.callRemoteMethods(null, new MethodCall(REMOVE, key),
                                   new RequestOptions(synchronous? ResponseMode.GET_ALL : ResponseMode.GET_NONE, call_timeout));
            if(l1_cache != null)
                l1_cache.remove(key);
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("remove() failed", t);
        }
    }


    /**
     * Removes all keys and values in the L2 and L1 caches
     */
    @ManagedOperation
    public void clear() {
        Set<K> keys=new HashSet<>(l2_cache.getInternalMap().keySet());
        mcastClear(keys, false);
    }

    public V _put(K key, V val, short repl_count, long timeout) {
        return _put(key, val, repl_count, timeout, false);
    }

    /**
     *
     * @param key
     * @param val
     * @param repl_count
     * @param timeout
     * @param force Skips acceptance checking and simply adds the key/value
     * @return
     */
    public V _put(K key, V val, short repl_count, long timeout, boolean force) {

        if(!force) {

            // check if we need to host the data
            boolean accept=repl_count == -1;

            if(!accept) {
                if(view != null && repl_count >= view.size()) {
                    accept=true;
                }
                else {
                    List<Address> selected_hosts=hash_function != null? hash_function.hash(key, repl_count) : null;
                    if(selected_hosts != null) {
                        if(log.isTraceEnabled())
                            log.trace("local=" + local_addr + ", hosts=" + selected_hosts);
                        for(Address addr: selected_hosts) {
                            if(addr.equals(local_addr)) {
                                accept=true;
                                break;
                            }
                        }
                    }
                    if(!accept)
                        return null;
                }
            }
        }

        if(log.isTraceEnabled())
            log.trace("_put(" + key + ", " + val + ", " + repl_count + ", " + timeout + ")");

        Value<V> value=new Value<>(val, repl_count);
        Value<V> retval=l2_cache.put(key, value, timeout);

        if(l1_cache != null)
            l1_cache.remove(key);

        notifyChangeListeners();

        return retval != null? retval.getVal() : null;
    }

    public Cache.Value<Value<V>> _get(K key) {
        if(log.isTraceEnabled())
            log.trace("_get(" + key + ")");
        return l2_cache.getEntry(key);
    }

    public V _remove(K key) {
        if(log.isTraceEnabled())
            log.trace("_remove(" + key + ")");
        Value<V> retval=l2_cache.remove(key);
        if(l1_cache != null)
            l1_cache.remove(key);
        notifyChangeListeners();
        return retval != null? retval.getVal() : null;
    }


    public void _removeMany(Set<K> keys) {
        if(log.isTraceEnabled())
            log.trace("_removeMany(): " + keys.size() + " entries");
        keys.forEach(this::_remove);
    }





    public void viewAccepted(final View new_view) {
        final List<Address> old_nodes=this.view != null? new ArrayList<>(this.view.getMembers()) : null;

        this.view=new_view;
        if(log.isDebugEnabled())
            log.debug("new view: " + new_view);

        if(hash_function != null)
            hash_function.installNodes(new_view.getMembers());

        for(Receiver r: receivers)
            r.viewAccepted(new_view);

        if(old_nodes != null) {
            timer.schedule(() -> rebalance(old_nodes, new ArrayList<>(new_view.getMembers())), 100, TimeUnit.MILLISECONDS);
        }
    }


    public void changed() {
        notifyChangeListeners();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        if(l1_cache != null)
            sb.append("L1 cache: " + l1_cache.getSize() + " entries");
        sb.append("\nL2 cache: " + l2_cache.getSize() + " entries()");
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

    private void notifyChangeListeners() {
        for(ChangeListener l: change_listeners) {
            try {
                l.changed();
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed notifying change listener", t);
            }
        }
    }



    private void rebalance(List<Address> old_nodes, List<Address> new_nodes) {
        HashFunction<K> old_func=hash_function_factory.create();
        old_func.installNodes(old_nodes);

        HashFunction<K> new_func=hash_function_factory.create();
        new_func.installNodes(new_nodes);

        boolean is_coord=Util.isCoordinator(ch);

        List<K> keys=new ArrayList<>(l2_cache.getInternalMap().keySet());

        for(K key: keys) {
            Cache.Value<Value<V>> val=l2_cache.getEntry(key);
            if(log.isTraceEnabled())
                log.trace("==== rebalancing " + key);
            if(val == null) {
                if(log.isWarnEnabled())
                    log.warn(key + " has no value associated; ignoring");
                continue;
            }
            Value<V> tmp=val.getValue();
            if(tmp == null) {
                if(log.isWarnEnabled())
                    log.warn(key + " has no value associated; ignoring");
                continue;
            }
            V real_value=tmp.getVal();
            short repl_count=tmp.getReplicationCount();
            List<Address> new_mbrs=Util.newMembers(old_nodes, new_nodes);

            if(repl_count == -1) {
                if(is_coord) {
                    for(Address new_mbr: new_mbrs) {
                        move(new_mbr, key, real_value, repl_count, val.getTimeout(), false);
                    }
                }
            }
            else if(repl_count == 1) {
                List<Address> tmp_nodes=new_func.hash(key, repl_count);
                if(!tmp_nodes.isEmpty()) {
                    Address mbr=tmp_nodes.get(0);
                    if(!mbr.equals(local_addr)) {
                        move(mbr, key, real_value, repl_count, val.getTimeout(), false);
                        _remove(key);
                    }
                }
            }
            else if(repl_count > 1) {
                List<Address> tmp_old=old_func.hash(key, repl_count);
                List<Address> tmp_new=new_func.hash(key, repl_count);

                if(log.isTraceEnabled())
                    log.trace("old nodes: " + tmp_old + "\nnew nodes: " + tmp_new);
                if(Objects.equals(tmp_old, tmp_new))
                    continue;
                mcastPut(key, real_value, repl_count, val.getTimeout(), false);
                if(tmp_new != null && !tmp_new.contains(local_addr)) {
                    _remove(key);
                }
            }
            else {
                throw new IllegalStateException("replication count is invalid (" + repl_count + ")");
            }
        }
    }

    public void mcastEntries() {
        for(Map.Entry<K,Cache.Value<Value<V>>> entry: l2_cache.entrySet()) {
            K key=entry.getKey();
            Cache.Value<Value<V>> val=entry.getValue();
            if(val == null) {
                if(log.isWarnEnabled())
                    log.warn(key + " has no value associated; ignoring");
                continue;
            }
            Value<V> tmp=val.getValue();
            if(tmp == null) {
                if(log.isWarnEnabled())
                    log.warn(key + " has no value associated; ignoring");
                continue;
            }
            V real_value=tmp.getVal();
            short repl_count=tmp.getReplicationCount();

            if(repl_count > 1) {
                _remove(key);
                mcastPut(key, real_value, repl_count, val.getTimeout(), false);
            }
        }
    }

    private void mcastPut(K key, V val, short repl_count, long caching_time, boolean synchronous) {
        try {
            ResponseMode mode=synchronous? ResponseMode.GET_ALL : ResponseMode.GET_NONE;
            disp.callRemoteMethods(null, new MethodCall(PUT, key, val, repl_count, caching_time),
                                   new RequestOptions(mode, call_timeout));
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("put() failed", t);
        }
    }

    private void mcastClear(Set<K> keys, boolean synchronous) {
        try {
            ResponseMode mode=synchronous? ResponseMode.GET_ALL : ResponseMode.GET_NONE;
            disp.callRemoteMethods(null, new MethodCall(REMOVE_MANY, keys), new RequestOptions(mode, call_timeout));
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("clear() failed", t);
        }
    }


    private void move(Address dest, K key, V val, short repl_count, long caching_time, boolean synchronous) {
        try {
            ResponseMode mode=synchronous? ResponseMode.GET_ALL : ResponseMode.GET_NONE;
            disp.callRemoteMethod(dest, new MethodCall(PUT_FORCE, key, val, repl_count, caching_time, true),
                                  new RequestOptions(mode, call_timeout));
        }
        catch(Throwable t) {
            if(log.isWarnEnabled())
                log.warn("move() failed", t);
        }
    }


    public interface ChangeListener {
        void changed();
    }
    
    public static class ConsistentHashFunction<K> implements HashFunction<K> {
        private final SortedMap<Short,Address> nodes=new TreeMap<>();
        private final static int HASH_SPACE=2048; // must be > max number of nodes in a cluster and a power of 2
        private final static int FACTOR=3737; // to better spread the node out across the space

        public List<Address> hash(K key, short replication_count) {
            int index=Math.abs(key.hashCode() & (HASH_SPACE - 1));
            
            Set<Address> results=new LinkedHashSet<>();

            SortedMap<Short, Address> tailmap=nodes.tailMap((short)index);
            int count=0;

            for(Map.Entry<Short,Address> entry: tailmap.entrySet()) {
                Address val=entry.getValue();
                results.add(val);
                if(++count >= replication_count)
                    break;
            }

            if(count < replication_count) {
                for(Map.Entry<Short,Address> entry: nodes.entrySet()) {
                    Address val=entry.getValue();
                    results.add(val);
                    if(++count >= replication_count)
                        break;
                }
            }
            return new ArrayList<>(results);
        }

        public void installNodes(List<Address> new_nodes) {
            nodes.clear();
            for(Address node: new_nodes) {
                int hash=Math.abs((node.hashCode() * FACTOR) & (HASH_SPACE - 1));
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

    }




    public static class Value<V> implements Serializable {
        private final V val;
        private final short replication_count;
        private static final long serialVersionUID=-2892941069742740027L;


        public Value(V val, short replication_count) {
            this.val=val;
            this.replication_count=replication_count;
        }

        public V getVal() {
            return val;
        }


        public short getReplicationCount() {
            return replication_count;
        }

        public String toString() {
            return val + " (" + replication_count + ")";
        }
    }

}
