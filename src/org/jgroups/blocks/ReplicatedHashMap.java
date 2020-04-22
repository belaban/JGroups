package org.jgroups.blocks;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link java.util.concurrent.ConcurrentMap} with replication of the contents across a cluster.
 * Any change to the hashmap (clear(), put(), remove() etc) will transparently be propagated to all replicas in the group.
 * All read-only methods will always access the local replica.
 * <p>
 * Keys and values added to the hashmap <em>must be serializable</em>, the reason being that they will be sent
 * across the network to all replicas of the group.<p/>
 * A {@code ReplicatedHashMap} allows one to implement a distributed naming service in just a couple of lines.
 * <p>
 * An instance of this class will contact an existing member of the group to fetch its initial state.
 *
 * @author Bela Ban
 */
public class ReplicatedHashMap<K, V> extends
        AbstractMap<K, V> implements ConcurrentMap<K, V>, Receiver, ReplicatedMap<K, V>, Closeable {

    public interface Notification<K, V> {
        void entrySet(K key, V value);

        void entryRemoved(K key);

        void viewChange(View view, java.util.List<Address> mbrs_joined, java.util.List<Address> mbrs_left);

        void contentsSet(Map<K, V> new_entries);

        void contentsCleared();
    }

    private static final short PUT = 1;
    private static final short PUT_IF_ABSENT = 2;
    private static final short PUT_ALL = 3;
    private static final short REMOVE = 4;
    private static final short REMOVE_IF_EQUALS = 5;
    private static final short REPLACE_IF_EXISTS = 6;
    private static final short REPLACE_IF_EQUALS = 7;
    private static final short CLEAR = 8;

    protected static Map<Short, Method> methods;

    static {
        try {
            methods = new HashMap<>(8);
            methods.put(PUT, ReplicatedHashMap.class.getMethod("_put",
                                                               Object.class,
                                                               Object.class));
            methods.put(PUT_IF_ABSENT, ReplicatedHashMap.class.getMethod("_putIfAbsent",
                                                                         Object.class,
                                                                         Object.class));
            methods.put(PUT_ALL, ReplicatedHashMap.class.getMethod("_putAll", Map.class));
            methods.put(REMOVE, ReplicatedHashMap.class.getMethod("_remove", Object.class));
            methods.put(REMOVE_IF_EQUALS, ReplicatedHashMap.class.getMethod("_remove",
                                                                            Object.class,
                                                                            Object.class));
            methods.put(REPLACE_IF_EXISTS, ReplicatedHashMap.class.getMethod("_replace",
                                                                             Object.class,
                                                                             Object.class));
            methods.put(REPLACE_IF_EQUALS, ReplicatedHashMap.class.getMethod("_replace",
                                                                             Object.class,
                                                                             Object.class,
                                                                             Object.class));
            methods.put(CLEAR, ReplicatedHashMap.class.getMethod("_clear"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private final JChannel channel;
    protected RpcDispatcher disp = null;
    private String cluster_name = null;
    // to be notified when mbrship changes
    private final Set<Notification> notifs = new CopyOnWriteArraySet<>();
    private final List<Address> members = new ArrayList<>(); // keeps track of all DHTs

    protected final RequestOptions call_options = new RequestOptions(ResponseMode.GET_NONE, 5000);

    protected final Log log = LogFactory.getLog(this.getClass());

    /**
     * wrapped map instance
     */
    protected ConcurrentMap<K, V> map = null;

    /**
     * Constructs a new ReplicatedHashMap with channel. Call {@link #start(long)} to start this map.
     */
    public ReplicatedHashMap(JChannel channel) {
        this.channel = channel;
        this.map = new ConcurrentHashMap<>();
        init();
    }

    /**
     * Constructs a new ReplicatedHashMap using provided map instance.
     */
    public ReplicatedHashMap(ConcurrentMap<K, V> map, JChannel channel) {
        if (channel == null) {
            throw new IllegalArgumentException("Cannot create ReplicatedHashMap with null channel");
        }
        if (map == null) {
            throw new IllegalArgumentException("Cannot create ReplicatedHashMap with null map");
        }
        this.map = map;
        this.cluster_name = channel.getClusterName();
        this.channel = channel;
        init();
    }

    protected final void init() {
        disp=new RpcDispatcher(channel, this).setMethodLookup(id -> methods.get(id));
        disp.setReceiver(this);
    }

    public boolean isBlockingUpdates() {
        return call_options.mode() == ResponseMode.GET_ALL;
    }

    /**
     * Whether updates across the cluster should be asynchronous (default) or synchronous)
     *
     * @param blocking_updates
     */
    public void setBlockingUpdates(boolean blocking_updates) {
        call_options.mode(blocking_updates ? ResponseMode.GET_ALL : ResponseMode.GET_NONE);
    }

    /**
     * The timeout (in milliseconds) for blocking updates
     */
    public long getTimeout() {
        return call_options.timeout();
    }

    /**
     * Sets the cluster call timeout (until all acks have been received)
     *
     * @param timeout The timeout (in milliseconds) for blocking updates
     */
    public void setTimeout(long timeout) {
        call_options.timeout(timeout);
    }

    /**
     * Fetches the state
     *
     * @param state_timeout
     */
    public final void start(long state_timeout) throws Exception {
        channel.getState(null, state_timeout);
    }

    public Address getLocalAddress() {
        return channel != null ? channel.getAddress() : null;
    }

    public String getClusterName() {
        return cluster_name;
    }

    public JChannel getChannel() {
        return channel;
    }

    public void addNotifier(Notification n) {
        if (n != null) {
            notifs.add(n);
        }
    }

    public void removeNotifier(Notification n) {
        if (n != null) {
            notifs.remove(n);
        }
    }

    public void stop() {
        if (disp != null) {
            disp.stop();
            disp = null;
        }
        Util.close(channel);
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    /**
     * Maps the specified key to the specified value in this table. Neither the key nor the value can be null. <p/>
     * <p>
     * The value can be retrieved by calling the <tt>get</tt> method with a key that is equal to the original key.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(K key, V value) {
        V prev_val = get(key);
        try {
            MethodCall call = new MethodCall(PUT, key, value);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("put(" + key + ", " + value + ") failed", e);
        }
        return prev_val;
    }

    /**
     * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(K key, V value) {
        V prev_val = get(key);
        try {
            MethodCall call = new MethodCall(PUT_IF_ABSENT, key, value);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("putIfAbsent(" + key + ", " + value + ") failed", e);
        }
        return prev_val;
    }

    /**
     * Copies all of the mappings from the specified map to this one. These
     * mappings replace any mappings that this map had for any of the keys
     * currently in the specified map.
     *
     * @param m mappings to be stored in this map
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            MethodCall call = new MethodCall(PUT_ALL, m);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Throwable t) {
            throw new RuntimeException("putAll() failed", t);
        }
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        try {
            MethodCall call = new MethodCall(CLEAR);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("clear() failed", e);
        }
    }

    /**
     * Removes the key (and its corresponding value) from this map. This method
     * does nothing if the key is not in the map.
     *
     * @param key the key that needs to be removed
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key is null
     */
    public V remove(Object key) {
        V retval = get(key);
        try {
            MethodCall call = new MethodCall(REMOVE, key);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("remove(" + key + ") failed", e);
        }
        return retval;
    }

    /**
     * @throws NullPointerException if the specified key is null
     */
    public boolean remove(Object key, Object value) {
        Object val = get(key);
        boolean removed =Objects.equals(val, value);
        try {
            MethodCall call = new MethodCall(REMOVE_IF_EQUALS, key, value);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("remove(" + key + ", " + value + ") failed", e);
        }
        return removed;
    }

    /**
     * @throws NullPointerException if any of the arguments are null
     */
    public boolean replace(K key, V oldValue, V newValue) {
        Object val = get(key);
        boolean replaced=Objects.equals(val, oldValue);
        try {
            MethodCall call = new MethodCall(REPLACE_IF_EQUALS, key, oldValue, newValue);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("replace(" + key
                                               + ", "
                                               + oldValue
                                               + ", "
                                               + newValue
                                               + ") failed", e);
        }
        return replaced;
    }

    /**
     * @return the previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V replace(K key, V value) {
        V retval = get(key);
        try {
            MethodCall call = new MethodCall(REPLACE_IF_EXISTS, key, value);
            disp.callRemoteMethods(null, call, call_options);
        } catch (Exception e) {
            throw new RuntimeException("replace(" + key + ", " + value + ") failed", e);
        }
        return retval;
    }

    /*------------------------ Callbacks -----------------------*/

    public V _put(K key, V value) {
        V retval = map.put(key, value);
        for (Notification notif : notifs)
            notif.entrySet(key, value);
        return retval;
    }

    public V _putIfAbsent(K key, V value) {
        V retval = map.putIfAbsent(key, value);
        for (Notification notif : notifs)
            notif.entrySet(key, value);
        return retval;
    }

    /**
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void _putAll(Map<? extends K, ? extends V> map) {
        if (map == null) {
            return;
        }
        // Calling the method below seems okay, but would result in ... deadlock !
        // The reason is that Map.putAll() calls put(), which we override, which results in
        // lock contention for the map.
        // ---> super.putAll(m); <--- CULPRIT !!!@#$%$
        // That said let's do it the stupid way:
        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            this.map.put(entry.getKey(), entry.getValue());
        }
        if (!map.isEmpty()) {
            for (Notification notif : notifs)
                notif.contentsSet(map);
        }
    }

    public void _clear() {
        map.clear();
        notifs.forEach(Notification::contentsCleared);
    }

    public V _remove(K key) {
        V retval = map.remove(key);
        if (retval != null) {
            for (Notification notif : notifs)
                notif.entryRemoved(key);
        }
        return retval;
    }

    public boolean _remove(K key, V value) {
        boolean removed = map.remove(key, value);
        if (removed) {
            for (Notification notif : notifs)
                notif.entryRemoved(key);
        }
        return removed;
    }

    public boolean _replace(K key, V oldValue, V newValue) {
        boolean replaced = map.replace(key, oldValue, newValue);
        if (replaced) {
            for (Notification notif : notifs)
                notif.entrySet(key, newValue);
        }
        return replaced;
    }

    public V _replace(K key, V value) {
        V retval = map.replace(key, value);
        for (Notification notif : notifs)
            notif.entrySet(key, value);
        return retval;
    }

    /*----------------------------------------------------------*/

    /*-------------------- State Exchange ----------------------*/
    public void getState(OutputStream ostream) throws Exception {
        HashMap<K, V> copy = new HashMap<>();
        for (Entry<K, V> entry : entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            copy.put(key, val);
        }
        try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(ostream, 1024))) {
            oos.writeObject(copy);
        }
    }

    public void setState(InputStream istream) throws Exception {
        HashMap<K, V> new_copy = null;
        try (ObjectInputStream ois = new ObjectInputStream(istream)) {
            new_copy = (HashMap<K, V>) ois.readObject();
        }
        if (new_copy != null) {
            _putAll(new_copy);
        }
        log.debug("state received successfully");
    }

    /*------------------- Membership Changes ----------------------*/

    public void viewAccepted(View new_view) {
        List<Address> new_mbrs = new_view.getMembers();
        if (new_mbrs != null) {
            sendViewChangeNotifications(new_view,
                                        new_mbrs,
                                        new ArrayList<>(members)); // notifies observers (joined, left)
            members.clear();
            members.addAll(new_mbrs);
        }
    }

    void sendViewChangeNotifications(View view, List<Address> new_mbrs, List<Address> old_mbrs) {
        if ((notifs.isEmpty()) || (old_mbrs == null) || (new_mbrs == null)) {
            return;
        }
        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        List<Address> joined = new_mbrs.stream().filter(mbr -> !old_mbrs.contains(mbr)).collect(Collectors.toList());
        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        List<Address> left = old_mbrs.stream().filter(mbr -> !new_mbrs.contains(mbr)).collect(Collectors.toList());
        notifs.forEach(notif -> notif.viewChange(view, joined, left));
    }


    /**
     * Creates a synchronized facade for a ReplicatedMap. All methods which
     * change state are invoked through a monitor. This is similar to
     * {@link java.util.Collections#synchronizedMap(Map)}, but also includes the replication
     * methods (starting with an underscore).
     *
     * @param map
     * @return
     */
    public static <K, V> ReplicatedMap<K, V> synchronizedMap(ReplicatedMap<K, V> map) {
        return new SynchronizedReplicatedMap<>(map);
    }

    private static final class SynchronizedReplicatedMap<K, V>
            implements ReplicatedMap<K, V> {
        private final ReplicatedMap<K, V> map;
        private final Object mutex;

        private SynchronizedReplicatedMap(ReplicatedMap<K, V> map) {
            this.map = map;
            this.mutex = this;
        }

        public int size() {
            synchronized (mutex) {
                return map.size();
            }
        }

        public boolean isEmpty() {
            synchronized (mutex) {
                return map.isEmpty();
            }
        }

        public boolean containsKey(Object key) {
            synchronized (mutex) {
                return map.containsKey(key);
            }
        }

        public boolean containsValue(Object value) {
            synchronized (mutex) {
                return map.containsValue(value);
            }
        }

        public V get(Object key) {
            synchronized (mutex) {
                return map.get(key);
            }
        }

        public V put(K key, V value) {
            synchronized (mutex) {
                return map.put(key, value);
            }
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            synchronized (mutex) {
                map.putAll(m);
            }
        }

        public void clear() {
            synchronized (mutex) {
                map.clear();
            }
        }

        public V putIfAbsent(K key, V value) {
            synchronized (mutex) {
                return map.putIfAbsent(key, value);
            }
        }

        public boolean remove(Object key, Object value) {
            synchronized (mutex) {
                return map.remove(key, value);
            }
        }

        public boolean replace(K key, V oldValue, V newValue) {
            synchronized (mutex) {
                return map.replace(key, oldValue, newValue);
            }
        }

        public V replace(K key, V value) {
            synchronized (mutex) {
                return map.replace(key, value);
            }
        }

        private Set<K> keySet = null;
        private Set<Entry<K, V>> entrySet = null;
        private Collection<V> values = null;

        public Set<K> keySet() {
            synchronized (mutex) {
                if (keySet == null) {
                    keySet = Collections.synchronizedSet(map.keySet());
                }
                return keySet;
            }
        }

        public Collection<V> values() {
            synchronized (mutex) {
                if (values == null) {
                    values = Collections.synchronizedCollection(map.values());
                }
                return values;
            }
        }

        public Set<Entry<K, V>> entrySet() {
            synchronized (mutex) {
                if (entrySet == null) {
                    entrySet = Collections.synchronizedSet(map.entrySet());
                }
                return entrySet;
            }
        }

        public V remove(Object key) {
            synchronized (mutex) {
                return map.remove(key);
            }
        }

        public V _put(K key, V value) {
            synchronized (mutex) {
                return map._put(key, value);
            }
        }

        public void _putAll(Map<? extends K, ? extends V> map) {
            synchronized (mutex) {
                this.map._putAll(map);
            }
        }

        public void _clear() {
            synchronized (mutex) {
                map._clear();
            }
        }

        public V _remove(K key) {
            synchronized (mutex) {
                return map._remove(key);
            }
        }

        public V _putIfAbsent(K key, V value) {
            synchronized (mutex) {
                return map._putIfAbsent(key, value);
            }
        }

        public boolean _remove(K key, V value) {
            synchronized (mutex) {
                return map._remove(key, value);
            }
        }

        public boolean _replace(K key, V oldValue, V newValue) {
            synchronized (mutex) {
                return map._replace(key, oldValue, newValue);
            }
        }

        public V _replace(K key, V value) {
            synchronized (mutex) {
                return map._replace(key, value);
            }
        }

        public String toString() {
            synchronized (mutex) {
                return map.toString();
            }
        }

        public int hashCode() {
            synchronized (mutex) {
                return map.hashCode();
            }
        }

        public boolean equals(Object obj) {
            synchronized (mutex) {
                return map.equals(obj);
            }
        }

    }
    // Delegate Methods

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public void clear() {
                ReplicatedHashMap.this.clear();
            }

            @Override
            public Iterator<Entry<K,V>> iterator() {
                final Iterator<Entry<K,V>> it=map.entrySet().iterator();
                return new Iterator<>() {
                    Entry<K,V> cur=null;

                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    public Entry<K,V> next() {
                        cur=it.next();
                        return cur;
                    }

                    public void remove() {
                        if(cur == null) {
                            throw new IllegalStateException();
                        }
                        ReplicatedHashMap.this.remove(cur.getKey());
                        cur=null;
                    }

                };

            }

            public int size() {
                return map.size();
            }
        };
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

}


