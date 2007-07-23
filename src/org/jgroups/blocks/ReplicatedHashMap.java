package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.persistence.CannotPersistException;
import org.jgroups.persistence.CannotRemoveException;
import org.jgroups.persistence.PersistenceFactory;
import org.jgroups.persistence.PersistenceManager;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.lang.reflect.Method;


/**
 * Provides the abstraction of a java.util.HashMap that is replicated across a cluster.
 * Any change to the hashmap (clear(), put(), remove() etc) will transparently be
 * propagated to all replicas in the group. All read-only methods will always access the local replica.<p>
 * Both keys and values added to the hashmap <em>must be serializable</em>, the reason
 * being that they will be sent across the network to all replicas of the group. Having said
 * this, it is now for example possible to add RMI remote objects to the hashtable as they
 * are derived from <code>java.rmi.server.RemoteObject</code> which in turn is serializable.
 * This allows to lookup shared distributed objects by their name and invoke methods on them,
 * regardless of one's onw location. A <code>DistributedHashMap</code> thus allows to
 * implement a distributed naming service in just a couple of lines.<p>
 * An instance of this class will contact an existing member of the group to fetch its
 * initial state (using the state exchange funclet <code>StateExchangeFunclet</code>.<p>
 * This class combines both {@link org.jgroups.blocks.ReplicatedHashtable} (asynchronous replication) and
 * {@link org.jgroups.blocks.DistributedHashtable} (synchronous replication) into one class
 * @author Bela Ban
 * @version $Id: ReplicatedHashMap.java,v 1.5 2007/07/23 09:28:09 belaban Exp $
 */
public class ReplicatedHashMap<K extends Serializable,V extends Serializable> extends HashMap<K,V> implements ExtendedMessageListener, ExtendedMembershipListener {




    public interface Notification<K extends Serializable,V extends Serializable> {
        void entrySet(K key, V value);

        void entryRemoved(K key);

        void viewChange(View view, Vector<Address> new_mbrs, Vector<Address> old_mbrs);

        void contentsSet(Map<K,V> new_entries);

        void contentsCleared();
    }

    protected static Map<Short, Method> methods;

    static {
        try {
            methods=new HashMap<Short,Method>(10);
            methods.put(new Short((short)1), ReplicatedHashMap.class.getMethod("_put", Serializable.class, Serializable.class));
            methods.put(new Short((short)2), ReplicatedHashMap.class.getMethod("_putAll", Map.class));
            methods.put(new Short((short)3), ReplicatedHashMap.class.getMethod("_remove", Object.class));
            methods.put(new Short((short)4), ReplicatedHashMap.class.getMethod("_clear"));
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private transient Channel channel;
    protected transient RpcDispatcher disp=null;
    private String cluster_name=null;
    private final transient Vector<Notification> notifs=new Vector<Notification>();  // to be notified when mbrship changes
    private final Vector<Address> members=new Vector<Address>(); // keeps track of all DHTs
    private transient boolean persistent=false; // whether to use PersistenceManager to save state
    private transient PersistenceManager persistence_mgr=null;

    /**
     * Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group
     */
    private transient boolean send_message=false;

    protected final transient Promise state_promise=new Promise();

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
     * @param state_timeout Max number of milliseconds to wait until state is
     *                      retrieved
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


    public ReplicatedHashMap(Channel channel, long state_timeout) {
        this(channel, false, state_timeout);
    }


    public ReplicatedHashMap(Channel channel, boolean persistent, long state_timeout) {
        this.cluster_name=channel.getClusterName();
        this.channel=channel;
        this.persistent=persistent;
        init(state_timeout);
    }


    protected final void init(long state_timeout) {
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
     *
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
            Boolean result=(Boolean)state_promise.getResult(state_timeout);
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
        if(!notifs.contains(n))
            notifs.addElement(n);
    }

    public void removeNotifier(Notification n) {
        if(notifs.contains(n))
            notifs.removeElement(n);
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
     * Maps the specified key to the specified value in the hashtable. Neither of both parameters can be null
     * @param key   - the hashtable key
     * @param value - the value
     * @return the previous value of the specified key in this hashtable, or null if it did not have one
     */
    public V put(K key, V value) {
        V prev_val=get(key);

        if(send_message == true) {
            try {
                MethodCall call=new MethodCall((short)1, new Object[]{key, value});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                throw new RuntimeException("put(" + key + ", " + value + ") failed", e);
            }
        }
        else {
            _put(key, value);
        }
        return prev_val;
    }


    /**
     * Copies all of the mappings from the specified Map to this hashmap.
     * These mappings will replace any mappings that this hashmap had for any of the keys currently in the specified Map.
     * @param m - Mappings to be stored in this map
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        if(send_message == true) {
            try {
                MethodCall call=new MethodCall((short)2, new Object[]{m});
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
     * Clears this hashtable so that it contains no keys
     */
    public void clear() {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                MethodCall call=new MethodCall((short)4, null);
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
     * Removes the key (and its corresponding value) from the hashmap.
     * @param key - the key to be removed.
     * @return the value to which the key had been mapped in this hashtable, or null if the key did not have a mapping.
     */
    public V remove(Object key) {
        V retval=get(key);

        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
                MethodCall call=new MethodCall((short)3, new Object[]{key});
                disp.callRemoteMethods(null, call, update_mode, timeout);
            }
            catch(Exception e) {
                 throw new RuntimeException("remove(" + key + ") failed", e);
            }
        }
        else {
            _remove(key);
            //don't have to do retval = super.remove(..) as is done at the beginning
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
            catch(CannotPersistException cannot_persist_ex) {
                if(log.isErrorEnabled()) log.error("failed persisting " + key + " + " +
                        value + ", exception=" + cannot_persist_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed persisting " + key + " + " +
                        value + ", exception=" + Util.printStackTrace(t));
            }
        }
        for(int i=0; i < notifs.size(); i++)
            notifs.elementAt(i).entrySet(key, value);
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

        if(persistent) {
            try {
                persistence_mgr.saveAll(map);
            }
            catch(CannotPersistException persist_ex) {
                if(log.isErrorEnabled()) log.error("failed persisting contents: " + persist_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed persisting contents: " + t);
            }
        }
        for(int i=0; i < notifs.size(); i++)
            notifs.elementAt(i).contentsSet(map);
    }


    public void _clear() {
        super.clear();
        if(persistent) {
            try {
                persistence_mgr.clear();
            }
            catch(CannotRemoveException cannot_remove_ex) {
                if(log.isErrorEnabled()) log.error("failed clearing contents, exception=" + cannot_remove_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed clearing contents, exception=" + t);
            }
        }
        for(int i=0; i < notifs.size(); i++)
            notifs.elementAt(i).contentsCleared();
    }


    public V _remove(Object key) {
        V retval=super.remove(key);
        if(persistent) {
            try {
                persistence_mgr.remove((Serializable)key);
            }
            catch(CannotRemoveException cannot_remove_ex) {
                if(log.isErrorEnabled()) log.error("failed clearing contents, exception=" + cannot_remove_ex);
            }
            catch(Throwable t) {
                if(log.isErrorEnabled()) log.error("failed clearing contents, exception=" + t);
            }
        }
        for(Notification notif: notifs)
            notif.entryRemoved((K)key);

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
        Notification n;

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

        for(int i=0; i < notifs.size(); i++) {
            n=notifs.elementAt(i);
            n.viewChange(view, joined, left);
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

}