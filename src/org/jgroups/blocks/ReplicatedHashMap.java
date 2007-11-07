package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.persistence.PersistenceFactory;
import org.jgroups.persistence.PersistenceManager;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * Subclass of a {@link java.util.concurrent.ConcurrentHashMap} with replication of the contents across a cluster.
 * Any change to the hashmap (clear(), put(), remove() etc) will transparently be
 * propagated to all replicas in the group. All read-only methods will always access the local replica.<p>
 * Keys and values added to the hashmap <em>must be serializable</em>, the reason
 * being that they will be sent across the network to all replicas of the group. Having said
 * this, it is now for example possible to add RMI remote objects to the hashtable as they
 * are derived from <code>java.rmi.server.RemoteObject</code> which in turn is serializable.
 * This allows to lookup shared distributed objects by their name and invoke methods on them,
 * regardless of one's onw location. A <code>ReplicatedHashMap</code> thus allows to
 * implement a distributed naming service in just a couple of lines.<p>
 * An instance of this class will contact an existing member of the group to fetch its
 * initial state.<p>
 * This class combines both {@link org.jgroups.blocks.ReplicatedHashtable} (asynchronous replication) and
 * {@link org.jgroups.blocks.DistributedHashtable} (synchronous replication) into one class
 * @author Bela Ban
 * @version $Id: ReplicatedHashMap.java,v 1.12 2007/11/07 09:39:25 belaban Exp $
 */
public class ReplicatedHashMap<K extends Serializable,V extends Serializable> extends ConcurrentHashMap<K,V> implements ExtendedReceiver, ReplicatedMap<K,V> {
    private static final long serialVersionUID=-5317720987340048547L;


    public interface Notification<K extends Serializable,V extends Serializable> {
        void entrySet(K key, V value);

        void entryRemoved(K key);

        void viewChange(View view, Vector<Address> new_mbrs, Vector<Address> old_mbrs);

        void contentsSet(Map<K,V> new_entries);

        void contentsCleared();
    }

    private static final short PUT               = 1;
    private static final short PUT_IF_ABSENT     = 2;
    private static final short PUT_ALL           = 3;
    private static final short REMOVE            = 4;
    private static final short REMOVE_IF_EQUALS  = 5;
    private static final short REPLACE_IF_EXISTS = 6;
    private static final short REPLACE_IF_EQUALS = 7;
    private static final short CLEAR             = 8;


    protected static Map<Short, Method> methods;

    static {
        try {
            methods=new HashMap<Short,Method>(8);
            methods.put(PUT, ReplicatedHashMap.class.getMethod("_put", Serializable.class, Serializable.class));
            methods.put(PUT_IF_ABSENT, ReplicatedHashMap.class.getMethod("_putIfAbsent", Serializable.class, Serializable.class));
            methods.put(PUT_ALL, ReplicatedHashMap.class.getMethod("_putAll", Map.class));
            methods.put(REMOVE, ReplicatedHashMap.class.getMethod("_remove", Object.class));
            methods.put(REMOVE_IF_EQUALS, ReplicatedHashMap.class.getMethod("_remove", Object.class, Object.class));
            methods.put(REPLACE_IF_EXISTS, ReplicatedHashMap.class.getMethod("_replace", Serializable.class, Serializable.class));
            methods.put(REPLACE_IF_EQUALS, ReplicatedHashMap.class.getMethod("_replace", Serializable.class, Serializable.class, Serializable.class));
            methods.put(CLEAR, ReplicatedHashMap.class.getMethod("_clear"));
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private transient Channel channel;
    protected transient RpcDispatcher disp=null;
    private String cluster_name=null;
    // to be notified when mbrship changes
    private final transient Set<Notification> notifs=new CopyOnWriteArraySet<Notification>();
    private final Vector<Address> members=new Vector<Address>(); // keeps track of all DHTs
    private transient boolean persistent=false; // whether to use PersistenceManager to save state
    private transient PersistenceManager persistence_mgr=null;

    /**
     * Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group
     */
    private transient boolean send_message=false;

    protected final transient Promise<Boolean> state_promise=new Promise<Boolean>();

    /**
     * Whether updates across the cluster should be asynchronous (default) or synchronous)
     */
    protected int update_mode=GroupRequest.GET_NONE;

    /**
     * For blocking updates only: the max time to wait (0 == forever)
     */
    protected long timeout=5000;

    protected final Log log=LogFactory.getLog(this.getClass());


    /**
     * Creates a ReplicatedHashMap
     * @param clustername   The name of the group to join
     * @param factory       The ChannelFactory which will be used to create a channel
     * @param properties    The property string to be used to define the channel. This will override the properties of
     *                      the factory. If null, then the factory properties will be used
     * @param state_timeout The time to wait until state is retrieved in milliseconds. A value of 0 means wait forever.
     */
    public ReplicatedHashMap(String clustername, ChannelFactory factory, String properties, long state_timeout)
            throws ChannelException {
        this.cluster_name=clustername;
        if(factory != null) {
            channel=properties != null? factory.createChannel(properties) : factory.createChannel();
        }
        else {
            channel=new JChannel(properties);
        }
        disp=new RpcDispatcher(channel, this, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return methods.get(id);
            }
        });
        channel.connect(clustername);
        start(state_timeout);
    }

    /**
     * Creates a ReplicatedHashMap. Optionally the contents can be saved to
     * persistemt storage using the {@link org.jgroups.persistence.PersistenceManager}.
     * @param clustername   Name of the group to join
     * @param factory       Instance of a ChannelFactory to create the channel
     * @param properties    Protocol stack properties. This will override the properties of the factory. If
     *                      null, then the factory properties will be used
     * @param persistent    Whether the contents should be persisted
     * @param state_timeout Max number of milliseconds to wait until the state is retrieved
     */
    public ReplicatedHashMap(String clustername, ChannelFactory factory, String properties,
                             boolean persistent, long state_timeout)
            throws ChannelException {
        this.cluster_name=clustername;
        this.persistent=persistent;
        if(factory != null) {
            channel=properties != null? factory.createChannel(properties) : factory.createChannel();
        }
        else {
            channel=new JChannel(properties);
        }
        disp=new RpcDispatcher(channel, this, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return methods.get(id);
            }
        });
        channel.connect(clustername);
        start(state_timeout);
    }


    public ReplicatedHashMap(Channel channel) {
        this(channel, false);
    }


    public ReplicatedHashMap(Channel channel, boolean persistent) {
        this.cluster_name=channel.getClusterName();
        this.channel=channel;
        this.persistent=persistent;
        init();
    }


    protected final void init() {
        disp=new RpcDispatcher(channel, this, this, this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return methods.get(id);
            }
        });

        // Changed by bela (jan 20 2003): start() has to be called by user (only when providing
        // own channel). First, Channel.connect() has to be called, then start().
        // start(state_timeout);
    }


    public boolean isBlockingUpdates() {
        return update_mode == GroupRequest.GET_ALL;
    }

    /**
     * Whether updates across the cluster should be asynchronous (default) or synchronous)
     * @param blocking_updates
     */
    public void setBlockingUpdates(boolean blocking_updates) {
        this.update_mode=blocking_updates? GroupRequest.GET_ALL : GroupRequest.GET_NONE;
    }

    /**
     * The timeout (in milliseconds) for blocking updates
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the cluster call timeout (until all acks have been received)
     * @param timeout The timeout (in milliseconds) for blocking updates
     */
    public void setTimeout(long timeout) {
        this.timeout=timeout;
    }

    /**
     * Fetches the state
     * @param state_timeout
     * @throws org.jgroups.ChannelClosedException
     * @throws org.jgroups.ChannelNotConnectedException
     *
     */
    public final void start(long state_timeout) throws ChannelClosedException, ChannelNotConnectedException {
        boolean rc;
        if(persistent) {
            if(log.isInfoEnabled()) log.info("fetching state from database");
            try {
                persistence_mgr=PersistenceFactory.getInstance().createManager();
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled()) log.error("failed creating PersistenceManager, " +
                        "turning persistency off. Exception: " + Util.printStackTrace(ex));
                persistent=false;
            }
        }

        state_promise.reset();
        rc=channel.getState(null, state_timeout);
        if(rc) {
            if(log.isInfoEnabled()) log.info("state was retrieved successfully, waiting for setState()");
            Boolean result=state_promise.getResult(state_timeout);
            if(result == null) {
                if(log.isErrorEnabled()) log.error("setState() never got called");
            }
            else {
                if(log.isInfoEnabled()) log.info("setState() was called");
            }
        }
        else {
            if(log.isInfoEnabled()) log.info("state could not be retrieved (first member)");
            if(persistent) {
                if(log.isInfoEnabled()) log.info("fetching state from database");
                try {
                    Map<K,V> m=persistence_mgr.retrieveAll();
                    if(m != null) {
                        K key;
                        V val;
                        for(Map.Entry<K,V> entry: m.entrySet()) {
                            key=entry.getKey();
                            val=entry.getValue();
                            if(log.isTraceEnabled()) log.trace("inserting " + key + " --> " + val);
                            put(key, val);  // will replicate key and value
                        }
                    }
                }
                catch(Throwable ex) {
                    if(log.isErrorEnabled()) log.error("failed creating PersistenceManager, " +
                            "turning persistency off. Exception: " + Util.printStackTrace(ex));
                    persistent=false;
                }
            }
        }
    }


    public Address getLocalAddress() {
        return channel != null? channel.getLocalAddress() : null;
    }

    public String getClusterName() {
        return cluster_name;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean getPersistent() {
        return persistent;
    }

    public void setPersistent(boolean p) {
        persistent=p;
    }


    public void setDeadlockDetection(boolean flag) {
        if(disp != null)
            disp.setDeadlockDetection(flag);
    }

    public void addNotifier(Notification n) {
        if(n != null)
            notifs.add(n);
    }

    public void removeNotifier(Notification n) {
        if(n != null)
            notifs.remove(n);
    }

    public void stop() {
        if(disp != null) {
            disp.stop();
            disp=null;
        }
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    /**
     * Maps the specified key to the specified value in this table.
     * Neither the key nor the value can be null.
     * <p/>
     * <p> The value can be retrieved by calling the <tt>get</tt> method
     * with a key that is equal to the original key.
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(K key, V value) {
        V prev_val=get(key);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(PUT, new Object[]{key, value});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("put(" + key + ", " + value + ") failed", e);
            }
        }
        else {
            return _put(key, value);
        }
        return prev_val;
    }



    /**
     * {@inheritDoc}
     * @return the previous value associated with the specified key,
     *         or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(K key, V value) {
        V prev_val=get(key);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(PUT_IF_ABSENT, new Object[]{key, value});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("putIfAbsent(" + key + ", " + value + ") failed", e);
            }
        }
        else {
            return _putIfAbsent(key, value);
        }
        return prev_val;
    }


    /**
     * Copies all of the mappings from the specified map to this one.
     * These mappings replace any mappings that this map had for any of the
     * keys currently in the specified map.
     * @param m mappings to be stored in this map
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(PUT_ALL, new Object[]{m});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Throwable t) {
                throw new RuntimeException("putAll() failed", t);
            }
        }
        else {
            _putAll(m);
        }
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(CLEAR, null);
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("clear() failed", e);
            }
        }
        else {
            _clear();
        }
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     * @param key the key that needs to be removed
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key is null
     */
    public V remove(Object key) {
        V retval=get(key);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(REMOVE, new Object[]{key});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                 throw new RuntimeException("remove(" + key + ") failed", e);
            }
        }
        else {
            return _remove(key);
        }
        return retval;
    }

    /**
     * {@inheritDoc}
     * @throws NullPointerException if the specified key is null
     */
    public boolean remove(Object key, Object value) {
        Object val=get(key);
        boolean removed=val != null && value != null && val.equals(value);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(REMOVE_IF_EQUALS, new Object[]{key, value});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("remove(" + key + ", " + value + ") failed", e);
            }
        }
        else {
            return _remove(key, value);
        }
        return removed;
    }


    /**
     * {@inheritDoc}
     * @throws NullPointerException if any of the arguments are null
     */
    public boolean replace(K key, V oldValue, V newValue) {
        Object val=get(key);
        boolean replaced=val != null && oldValue != null && val.equals(oldValue);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(REPLACE_IF_EQUALS, new Object[]{key, oldValue, newValue});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("replace(" + key + ", " + oldValue + ", " + newValue + ") failed", e);
            }
        }
        else {
            return _replace(key, oldValue,newValue);
        }
        return replaced;
    }

    /**
     * {@inheritDoc}
     * @return the previous value associated with the specified key,
     *         or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V replace(K key, V value) {
        V retval=get(key);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall(REPLACE_IF_EXISTS, new Object[]{key, value});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("replace(" + key + ", " + value + ") failed", e);
            }
        }
        else {
            return _replace(key, value);
        }
        return retval;
    }

    /*------------------------ Callbacks -----------------------*/

    public V _put(K key, V value) {
        V retval=super.put(key, value);
        if(persistent) {
            try {
                persistence_mgr.save(key, value);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed persisting " + key + " + " + value, t);
            }
        }
        for(Notification notif: notifs)
            notif.entrySet(key, value);
        return retval;
    }

    public V _putIfAbsent(K key, V value) {
        V retval=super.putIfAbsent(key, value);
        if(persistent) {
            try {
                persistence_mgr.save(key, value);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed persisting " + key + " + " + value, t);
            }
        }
        for(Notification notif: notifs)
            notif.entrySet(key, value);
        return retval;
    }


    /**
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void _putAll(Map<? extends K, ? extends V> map) {
        if(map == null)
            return;

        // Calling the method below seems okay, but would result in ... deadlock !
        // The reason is that Map.putAll() calls put(), which we override, which results in
        // lock contention for the map.

        // ---> super.putAll(m); <--- CULPRIT !!!@#$%$

        // That said let's do it the stupid way:
        for(Map.Entry<? extends K,? extends V> entry: map.entrySet()) {
            super.put(entry.getKey(), entry.getValue());
        }

        if(persistent && !map.isEmpty()) {
            try {
                persistence_mgr.saveAll(map);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed persisting contents", t);
            }
        }
        if(!map.isEmpty()) {
            for(Notification notif: notifs)
                notif.contentsSet(map);
        }
    }


    public void _clear() {
        super.clear();
        if(persistent) {
            try {
                persistence_mgr.clear();
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed clearing contents", t);
            }
        }
        for(Notification notif: notifs)
            notif.contentsCleared();
    }


    public V _remove(Object key) {
        V retval=super.remove(key);
        if(persistent && retval != null) {
            try {
                persistence_mgr.remove((Serializable)key);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed removing " + key, t);
            }
        }
        if(retval != null) {
            for(Notification notif: notifs)
                notif.entryRemoved((K)key);
        }

        return retval;
    }



    public boolean _remove(Object key, Object value) {
        boolean removed=super.remove(key, value);
        if(persistent && removed) {
            try {
                persistence_mgr.remove((Serializable)key);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed removing " + key, t);
            }
        }
        if(removed) {
            for(Notification notif: notifs)
                notif.entryRemoved((K)key);
        }
        return removed;
    }

    public boolean _replace(K key, V oldValue, V newValue) {
        boolean replaced=super.replace(key, oldValue, newValue);
        if(persistent && replaced) {
            try {
                persistence_mgr.save(key, newValue);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed saving " + key + ", " + newValue, t);
            }
        }
        if(replaced) {
            for(Notification notif: notifs)
                notif.entrySet(key, newValue);
        }
        return replaced;
    }

    public V _replace(K key, V value) {
        V retval=super.replace(key, value);
        if(persistent) {
            try {
                persistence_mgr.save(key, value);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed saving " + key + ", " + value, t);
            }
        }
        for(Notification notif: notifs)
            notif.entrySet(key, value);
        return retval;
    }

    /*----------------------------------------------------------*/

    /*-------------------- State Exchange ----------------------*/

    public void receive(Message msg) {
    }

    public byte[] getState() {
        K key;
        V val;
        Map<K,V> copy=new HashMap<K,V>();

        for(Map.Entry<K,V> entry: entrySet()) {
            key=entry.getKey();
            val=entry.getValue();
            copy.put(key, val);
        }
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception marshalling state: " + ex);
            return null;
        }
    }


    public void setState(byte[] new_state) {
        HashMap<K,V> new_copy;

        try {
            new_copy=(HashMap<K,V>)Util.objectFromByteBuffer(new_state);
            if(new_copy == null)
                return;
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception unmarshalling state: " + ex);
            return;
        }
        _putAll(new_copy);
        state_promise.setResult(Boolean.TRUE);
    }

    /*------------------- Membership Changes ----------------------*/

    public void viewAccepted(View new_view) {
        Vector<Address> new_mbrs=new_view.getMembers();

        if(new_mbrs != null) {
            sendViewChangeNotifications(new_view, new_mbrs, new Vector<Address>(members)); // notifies observers (joined, left)
            members.clear();
            members.addAll(new_mbrs);
        }
        //if size is bigger than one, there are more peers in the group
        //otherwise there is only one server.
        send_message=members.size() > 1;
    }


    /**
     * Called when a member is suspected
     */
    public void suspect(Address suspected_mbr) {
        ;
    }


    /**
     * Block sending and receiving of messages until ViewAccepted is called
     */
    public void block() {
    }


    void sendViewChangeNotifications(View view, Vector<Address> new_mbrs, Vector<Address> old_mbrs) {
        Vector<Address> joined, left;

        if((notifs.isEmpty()) || (old_mbrs == null) || (new_mbrs == null) ||
                (old_mbrs.isEmpty()) || (new_mbrs.isEmpty()))
            return;

        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        joined=new Vector<Address>();
        for(Address mbr: new_mbrs) {
            if(!old_mbrs.contains(mbr))
                joined.addElement(mbr);
        }

        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        left=new Vector<Address>();
        for(Address mbr: old_mbrs) {
            if(!new_mbrs.contains(mbr)) {
                left.addElement(mbr);
            }
        }

        for(Notification notif: notifs) {
            notif.viewChange(view, joined, left);
        }
    }




    public byte[] getState(String state_id) {
        // not implemented
        return null;
    }

    public void getState(OutputStream ostream) {
        K key;
        V val;
        HashMap<K,V> copy=new HashMap<K,V>();
        ObjectOutputStream oos=null;

        for(Map.Entry<K,V> entry: entrySet()) {
            key=entry.getKey();
            val=entry.getValue();
            copy.put(key, val);
        }
        try {
            oos=new ObjectOutputStream(ostream);
            oos.writeObject(copy);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception marshalling state: " + ex);
        }
        finally {
            Util.close(oos);
        }
    }

    public void getState(String state_id, OutputStream ostream) {
    }

    public void setState(String state_id, byte[] state) {
    }

    public void setState(InputStream istream) {
        HashMap<K,V> new_copy=null;
        ObjectInputStream ois=null;
        try {
            ois=new ObjectInputStream(istream);
            new_copy=(HashMap<K,V>)ois.readObject();
            ois.close();
        }
        catch(Throwable e) {
            e.printStackTrace();
            if(log.isErrorEnabled()) log.error("exception marshalling state: " + e);
        }
        finally {
            Util.close(ois);
        }
        if(new_copy != null)
            _putAll(new_copy);

        state_promise.setResult(Boolean.TRUE);
    }

    public void setState(String state_id, InputStream istream) {
    }

    public void unblock() {
    }


    /**
     * Creates a synchronized facade for a ReplicatedMap. All methods which change state are invoked through a monitor.
     * This is similar to {@Collections.synchronizedMap()}, but also includes the replication methods (starting
     * with an underscore).
     * @param map
     * @return
     */
    public static <K extends Serializable,V extends Serializable> ReplicatedMap<K,V> synchronizedMap(ReplicatedMap<K,V> map) {
        return new SynchronizedReplicatedMap<K,V>(map);
    }

    private static class SynchronizedReplicatedMap<K extends Serializable,V extends Serializable> implements ReplicatedMap<K,V> {
        private final ReplicatedMap<K,V> map;
        private final Object mutex;

        
        private SynchronizedReplicatedMap(ReplicatedMap<K, V> map) {
            this.map=map;
            this.mutex=this;
        }

        public int size() {
            synchronized(mutex) {
               return map.size();
            }
        }

        public boolean isEmpty() {
            synchronized(mutex) {
                return map.isEmpty();
            }
        }

        public boolean containsKey(Object key) {
            synchronized(mutex) {
                return map.containsKey(key);
            }
        }

        public boolean containsValue(Object value) {
            synchronized(mutex) {
                return map.containsValue(value);
            }
        }

        public V get(Object key) {
            synchronized(mutex) {
                return map.get(key);
            }
        }

        public V put(K key, V value) {
            synchronized(mutex) {
                return map.put(key, value);
            }
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            synchronized(mutex) {
                map.putAll(m);
            }
        }

        public void clear() {
            synchronized(mutex) {
                map.clear();
            }
        }

        public V putIfAbsent(K key, V value) {
            synchronized(mutex) {
                return map.putIfAbsent(key, value);
            }
        }

        public boolean remove(Object key, Object value) {
            synchronized(mutex) {
                return map.remove(key, value);
            }
        }

        public boolean replace(K key, V oldValue, V newValue) {
            synchronized(mutex) {
                return map.replace(key, oldValue, newValue);
            }
        }

        public V replace(K key, V value) {
            synchronized(mutex) {
                return map.replace(key, value);
            }
        }

        private Set<K> keySet=null;
        private Set<Map.Entry<K,V>> entrySet=null;
        private Collection<V> values=null;


        public Set<K> keySet() {
            synchronized(mutex) {
                if (keySet == null)
                    keySet=Collections.synchronizedSet(map.keySet());
                return keySet;
            }
        }

        public Collection<V> values() {
            synchronized(mutex) {
                if (values == null)
                    values=Collections.synchronizedCollection(map.values());
                return values;
            }
        }

        public Set<Entry<K, V>> entrySet() {
            synchronized(mutex) {
                if (entrySet == null)
                    entrySet=Collections.synchronizedSet(map.entrySet());
                return entrySet;
            }
        }

        public V remove(Object key) {
            synchronized(mutex) {
                return map.remove(key);
            }
        }

        public V _put(K key, V value) {
            synchronized(mutex) {
                return map._put(key, value);
            }
        }

        public void _putAll(Map<? extends K, ? extends V> map) {
            synchronized(mutex) {
                this.map._putAll(map);
            }
        }

        public void _clear() {
            synchronized(mutex) {
                map._clear();
            }
        }

        public V _remove(Object key) {
            synchronized(mutex) {
                return map._remove(key);
            }
        }

        public V _putIfAbsent(K key, V value) {
            synchronized(mutex) {
                return map._putIfAbsent(key, value);
            }
        }

        public boolean _remove(Object key, Object value) {
            synchronized(mutex) {
                return map._remove(key, value);
            }
        }

        public boolean _replace(K key, V oldValue, V newValue) {
            synchronized(mutex) {
                return map._replace(key, oldValue, newValue);
            }
        }

        public V _replace(K key, V value) {
            synchronized(mutex) {
                return map._replace(key, value);
            }
        }

        public String toString() {
            synchronized(mutex) {
                return map.toString();
            }
        }

        public int hashCode() {
            synchronized(mutex) {
                return map.hashCode();
            }
        }

        public boolean equals(Object obj) {
            synchronized(mutex) {
                return map.equals(obj);
            }
        }


    }


}