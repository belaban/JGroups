// $Id: TransactionalHashtable.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.TimeoutException;
import org.jgroups.log.Trace;
import org.jgroups.util.RWLock;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;




/*
 TODO:
 - handle view changes and release locks held by crashed members
 - implement lock acquisition timeouts on RWLock
 - implement lock lease timeouts on RWLock (may on subclass of RWLock ?)
 - implement state transfer with locking
 - a transaction has locks and a private workspace. the latter will be a copy of the entire hashmap
   (transaction mode == REPEATABLE_READ or SERIALIZABLE) or just a list of modifications (DIRTY_READS or 
   READ_COMMITTED mode)
 - return RspList for put(), putAll(), remove() and clear() ?
 - put read locks around e.g. get() 
 - implement Notification interface
 - handle MergeViews

*/


/**
 * Hashtable which replicates its contents to all members of the group. Reads and writes can optionally
 * be forced to acquire locks (r/w locks) to ensure total serializability between replicas. The update modes
 * can be chosen per method and are (in order of cost)
 * <ol>
 * <li>asynchronous (non-blocking) updates
 * <li>synchronous (blocking) updates (optionally with a timeout)
 * <li>synchronous (blocking) updates with locking
 * </ol>
 * This class needs to have a state transfer protocol present in the protocol stack used (e.g. pbcast.STATE_TRANSFER).
 * @author Bela Ban Nov 2002
 */
public class TransactionalHashtable extends HashMap implements ReplicationReceiver, MessageListener {
    protected ReplicationManager repl_mgr;
    protected Channel            channel=null;
    protected Address            local_addr=null;
    protected String             groupname="TransactionalHashtable-Group";
    protected String             properties=null;
    protected long               state_timeout=10000;            // number of milliseconds to wait for initial state
    protected boolean            default_sync_repl=false;        // default mode is asynchronous repl
    protected long               default_sync_repl_timeout=5000; // default timeout for synchronous replication
    protected long               lock_acquisition_timeout=5000;  // default lock acquisition timeout
    protected long               lock_lease_timeout=0    ;       // default lock lease timeout (forever)
    protected int                transaction_mode=Xid.DIRTY_READS; // default tx mode, used when no tx is defined explicitly
    protected RWLock             table_lock=new RWLock();        // lock on entire hashmap
    protected HashMap            row_locks=new HashMap();        // locks for individual rows (keys=row key, values=RWLock)
    protected boolean            auto_commit=false;              // commit after each upate (e.g. put()) ?
    protected List               notifs=new ArrayList();         // for Notification observers

    //public static final int      ASYNC           = 1;  // asynchronous replication
    //public static final int      SYNC            = 2;  // synchronous replication
    //public static final int      SYNC_WITH_LOCKS = 3;  // synchronous replication with locking



    public interface Notification {
        void entrySet(Object key, Object value);
        void entryRemoved(Object key);
        void viewChange(Vector new_mbrs, Vector old_mbrs);
    }

    
    /**
     * Thread local variable. Is initialized with a hashmap
     */
    private static ThreadLocal thread_local = new ThreadLocal() {
            protected synchronized Object initialValue() {
                return new HashMap();
            }
        };


    public void addNotifier(Notification n) {
        if(!notifs.contains(n))
            notifs.add(n);
    }



    /* ------------------------------ Overridden methods of HashMap --------------------------- */

    public TransactionalHashtable(String groupname,
                                  String properties,
                                  long   state_timeout) throws Exception {
        super();
        initChannel(groupname, properties, state_timeout);
    }


    public TransactionalHashtable(String groupname,
                                  String properties,
                                  long   state_timeout,
                                  Map    m) throws Exception {
        super(m);
        initChannel(groupname, properties, state_timeout);
        putAll(m); // replicate initial contents
    }
    
    public TransactionalHashtable(String groupname,
                                  String properties,
                                  long   state_timeout,
                                  int    initialCapacity) throws Exception {
        super(initialCapacity);
        initChannel(groupname, properties, state_timeout);
    }


    public TransactionalHashtable(String groupname,
                                  String properties,
                                  long   state_timeout,
                                  int    initialCapacity,
                                  float  loadFactor) throws Exception {
        super(initialCapacity, loadFactor);
        initChannel(groupname, properties, state_timeout);
    }

    
    public Object get(Object key) {
        return super.get(key); // TODO: implement
    }
    
    public boolean containsKey(Object key) {
        return super.containsKey(key); // TODO: implement
    }



    /**
     * Replicates the update to all members. Depending on the value of <code>default_sync_repl_timeout</code>
     * the update will be sent synchronously or asynchronously
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @param value The value to be set. Needs to be serializable. Can be null.
     * @return Object The previous value associated with the given key, or null if none was associated
     */
    public Object put(Object key, Object value) {
        return put(key, value, default_sync_repl, default_sync_repl_timeout);
    }


    /**
     * Replicates the update to all members. Depending on the value of the parameters the update will be
     * synchronous or asynchronous.
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @param value The value to be set. Needs to be serializable. Can be null.
     * @param synchronous If true the update will be synchronous, ie. the caller will block until all responses
     *                    have been received. If <code>timeout</code> is 0, we will block indefinitely (until all
     *                    responses have been received), otherwise the call is guaranteed to return after at most
     *                    <code>timeout</code> milliseconds. If false, the call will be asynchronous.
     * @param timeout The number of milliseconds to wait for a synchronous call. 0 means to wait forever.
     *                This parameter is not used if <code>synchronous</code> is false.
     * @return Object The previous value associated with the given key, or null if none was associated
     */
    public Object put(Object key, Object value, boolean synchronous, long timeout) {
        Data   data;
        Object retval;
        byte[] buf;
        
        data=new Data(Data.PUT, (Serializable)key, (Serializable)value);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.put()", "marshalling failure: " + ex);
            return null;
        }
        retval=get(key);

        repl_mgr.send(null,          // mcast to all members of the group
                      buf,           // marshalled data
                      synchronous,   // send asynchronously
                      timeout,       // timeout (not used on asynchronous send)
                      null,          // no transaction needed
                      null,          // no lock information needed
                      0,             // lock acquisition timeout not needed
                      0,             // lock lease timeout not needed
                      false);        // don't use locks
        return retval; // we don't care about the responses as this is an asynchronous call
    }


    /**
     * Replicates the update to all members, and use locks at each member to ensure serializability.
     * When a lock in a member cannot be acquired, a LockingException will be thrown. Typically the caller will
     * then abort the transaction (releasing all locks) and retry.<p>
     * This call can be one of many inside the same transaction, or it may be the only one. In the first case, the
     * caller is responsible to call commit() or rollback() once the transaction is done. In the latter case, the
     * transaction can be committed by setting <code>commit</code> to true.<br>
     * A transaction (Xid) is always associated with the current thread. If this call is invoked, and there is no
     * transaction associated with the current thread, a default transaction will be created. Otherwise the current
     * transaction will be used.
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @param value The value to be set. Needs to be serializable. Can be null.
     * @param sync_timeout Max number of milliseconds to wait for all responses. Note that this needs to be higher
     *                     than <code>lock_acquisition_timeout</code>. 0 means to wait forever for all responses.
     * @param lock_acquisition_timeout Number of milliseonds to wait until a lock becomes available. Needs to be lower
     *                                 than <code>sync_timeout</code>. 0 means to wait forever. <em>0 will block
     *                                 forever in case of deadlocks. Once we have deadlock detection in place,
     *                                 this parameter may be deprecated.</em>
     * @param lock_lease_timeout       Number of milliseonds until a lock is released automatically (if not released
     *                                 before). Not currently used.
     * @param commit                   If true the transaction will be committed after this call if the call was successful.
     * @return Object The previous value associated with the given key, or null if none was associated
     * @exception LockingException Throw when one or more of the members failed acquiring the lock within
     *                             <code>lock_acquisition_timeout</code> milliseconds
     * @exception TimeoutException Thrown when one or more of the members didn't send a response. LockingExceptions
     *                             take precedence over TimeoutExceptions, e.g. if we have both locking and timeout
     *                             exceptions, a LockingException will be thrown.
     */
    public Object put(Object key, Object value, long sync_timeout, 
                      long lock_acquisition_timeout, long lock_lease_timeout,
                      boolean commit) throws LockingException, TimeoutException {
        Data    data;
        Object  retval;
        byte[]  buf;
        Xid     curr_transaction;
        RspList rsps;
        Rsp     rsp;
        
        data=new Data(Data.PUT, (Serializable)key, (Serializable)value);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.put()", "marshalling failure: " + ex);
            return null;
        }
        retval=get(key);
        curr_transaction=getCurrentTransaction();
        if(curr_transaction == null) {
            Trace.info("TransactionalHashtable.putl()", "no transaction associated with current thread." + 
                       " Will create new transaction with transaction mode=" + Xid.modeToString(transaction_mode));
            try {
                begin(transaction_mode);
            }
            catch(Throwable ex) {
                Trace.error("TransactionalHashtable.putl()", "could not start new transaction: " + ex);
                return null;
            }
            curr_transaction=getCurrentTransaction();
        }
        
        rsps=repl_mgr.send(null,                     // mcast to all members of the group
                           buf,                      // marshalled data
                           true,                     // send synchronously
                           sync_timeout,             // timeout: max time to wait for all responses
                           curr_transaction,         // transaction required
                           null,                     // no lock information needed
                           lock_acquisition_timeout, // lock acquisition timeout not needed
                           lock_lease_timeout,       // lock lease timeout not needed
                           true);                    // don't use locks
        
        // now check results
        if(rsps == null) // should not happen
            Trace.error("TransactionalHashtable.putl()", "RspList of call is null");
        else
            checkResults(rsps); // may throw a LockingException or TimeoutException

        // if commit == true: commit transaction
        if(commit)
            commit(getCurrentTransaction());

        return retval;
    }



    public Object lput(Object key, Object value) throws LockingException, TimeoutException {
        return put(key, value, default_sync_repl_timeout,
                   lock_acquisition_timeout, lock_lease_timeout, auto_commit);
    }
    






    /**
     * Replicates the update to all members. Depending on the value of <code>default_sync_repl_timeout</code>
     * the update will be sent synchronously or asynchronously
     * @param m The map to be set. All entries need to be serializable. Cannot be null.
     */
    public void putAll(Map m) {
        putAll(m, default_sync_repl, default_sync_repl_timeout);
    }


    /**
     * Replicates the update to all members. Depending on the value of the parameters the update will be
     * synchronous or asynchronous.
     * @param m The map to be set. All entries need to be serializable. Cannot be null.
     * @param synchronous If true the update will be synchronous, ie. the caller will block until all responses
     *                    have been received. If <code>timeout</code> is 0, we will block indefinitely (until all
     *                    responses have been received), otherwise the call is guaranteed to return after at most
     *                    <code>timeout</code> milliseconds. If false, the call will be asynchronous.
     * @param timeout The number of milliseconds to wait for a synchronous call. 0 means to wait forever.
     *                This parameter is not used if <code>synchronous</code> is false.
     */
    public void putAll(Map m, boolean synchronous, long timeout) {
        Data   data;
        Object retval;
        byte[] buf;
        
        data=new Data(Data.PUT_ALL, m);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.putAll()", "marshalling failure: " + ex);
            return;
        }
        repl_mgr.send(null,          // mcast to all members of the group
                      buf,           // marshalled data
                      synchronous,   // send asynchronously
                      timeout,       // timeout (not used on asynchronous send)
                      null,          // no transaction needed
                      null,          // no lock information needed
                      0,             // lock acquisition timeout not needed
                      0,             // lock lease timeout not needed
                      false);        // don't use locks
    }


    /**
     * Replicated the update to all members, and use locks at each member to ensure serializability.
     * When a lock in a member cannot be acquired, a LockingException will be thrown. Typically the caller will
     * then abort the transaction (releasing all locks) and retry.<p>
     * This call can be one of many inside the same transaction, or it may be the only one. In the first case, the
     * caller is responsible to call commit() or rollback() once the transaction is done. In the latter case, the
     * transaction can be committed by setting <code>commit</code> to true.<br>
     * A transaction (Xid) is always associated with the current thread. If this call is invoked, and there is no
     * transaction associated with the current thread, a default transaction will be created. Otherwise the current
     * transaction will be used.
     * @param m The map to be set. All entries need to be serializable. Cannot be null.
     * @param sync_timeout Max number of milliseconds to wait for all responses. Note that this needs to be higher
     *                     than <code>lock_acquisition_timeout</code>. 0 means to wait forever for all responses.
     * @param lock_acquisition_timeout Number of milliseonds to wait until a lock becomes available. Needs to be lower
     *                                 than <code>sync_timeout</code>. 0 means to wait forever. <em>0 will block
     *                                 forever in case of deadlocks. Once we have deadlock detection in place,
     *                                 this parameter may be deprecated.</em>
     * @param lock_lease_timeout       Number of milliseonds until a lock is released automatically (if not released
     *                                 before). Not currently used.
     * @param commit                   If true the transaction will be committed after this call if the call was successful.
     * @exception LockingException Throw when one or more of the members failed acquiring the lock within
     *                             <code>lock_acquisition_timeout</code> milliseconds
     * @exception TimeoutException Thrown when one or more of the members didn't send a response. LockingExceptions
     *                             take precedence over TimeoutExceptions, e.g. if we have both locking and timeout
     *                             exceptions, a LockingException will be thrown.
     */
    public void putAll(Map m, long sync_timeout, 
                       long lock_acquisition_timeout, long lock_lease_timeout,
                       boolean commit) throws LockingException, TimeoutException {
        Data    data;
        byte[]  buf;
        Xid     curr_transaction;
        RspList rsps;
        Rsp     rsp;
        
        data=new Data(Data.PUT_ALL, (Serializable)m);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.putAll()", "marshalling failure: " + ex);
            return;
        }
        curr_transaction=getCurrentTransaction();
        if(curr_transaction == null) {
            Trace.info("TransactionalHashtable.putAlll()", "no transaction associated with current thread." + 
                       " Will create new transaction with transaction mode=" + Xid.modeToString(transaction_mode));
            try {
                begin(transaction_mode);
            }
            catch(Throwable ex) {
                Trace.error("TransactionalHashtable.putAll()", "could not start new transaction: " + ex);
                return;
            }
            curr_transaction=getCurrentTransaction();
        }
        
        rsps=repl_mgr.send(null,                     // mcast to all members of the group
                           buf,                      // marshalled data
                           true,                     // send synchronously
                           sync_timeout,             // timeout: max time to wait for all responses
                           curr_transaction,         // transaction required
                           null,                     // no lock information needed
                           lock_acquisition_timeout, // lock acquisition timeout not needed
                           lock_lease_timeout,       // lock lease timeout not needed
                           true);                    // don't use locks
        
        // now check results
        if(rsps == null) // should not happen
            Trace.error("TransactionalHashtable.putAlll()", "RspList of call is null");
        else
            checkResults(rsps); // may throw a LockingException or TimeoutException

        // if commit == true: commit transaction
        if(commit)
            commit(getCurrentTransaction());
    }



    public void lputAll(Map m) throws LockingException, TimeoutException {
        putAll(m, default_sync_repl_timeout,
               lock_acquisition_timeout, lock_lease_timeout, auto_commit);
    }




















    /**
     * Replicates the update to all members. Depending on the value of <code>default_sync_repl_timeout</code>
     * the update will be sent synchronously or asynchronously
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @return Object The previous value associated with the given key, or null if none was associated
     */
    public Object remove(Object key) {
        return remove(key, default_sync_repl, default_sync_repl_timeout);
    }


    /**
     * Replicates the update to all members. Depending on the value of the parameters the update will be
     * synchronous or asynchronous.
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @param synchronous If true the update will be synchronous, ie. the caller will block until all responses
     *                    have been received. If <code>timeout</code> is 0, we will block indefinitely (until all
     *                    responses have been received), otherwise the call is guaranteed to return after at most
     *                    <code>timeout</code> milliseconds. If false, the call will be asynchronous.
     * @param timeout The number of milliseconds to wait for a synchronous call. 0 means to wait forever.
     *                This parameter is not used if <code>synchronous</code> is false.
     * @return Object The previous value associated with the given key, or null if none was associated
     */
    public Object remove(Object key, boolean synchronous, long timeout) {
        Data   data;
        Object retval;
        byte[] buf;
        
        data=new Data(Data.REMOVE, (Serializable)key);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.remove()", "marshalling failure: " + ex);
            return null;
        }
        retval=get(key);

        repl_mgr.send(null,          // mcast to all members of the group
                      buf,           // marshalled data
                      synchronous,   // send asynchronously
                      timeout,       // timeout (not used on asynchronous send)
                      null,          // no transaction needed
                      null,          // no lock information needed
                      0,             // lock acquisition timeout not needed
                      0,             // lock lease timeout not needed
                      false);        // don't use locks
        return retval; // we don't care about the responses as this is an asynchronous call
    }


    /**
     * Replicated the update to all members, and use locks at each member to ensure serializability.
     * When a lock in a member cannot be acquired, a LockingException will be thrown. Typically the caller will
     * then abort the transaction (releasing all locks) and retry.<p>
     * This call can be one of many inside the same transaction, or it may be the only one. In the first case, the
     * caller is responsible to call commit() or rollback() once the transaction is done. In the latter case, the
     * transaction can be committed by setting <code>commit</code> to true.<br>
     * A transaction (Xid) is always associated with the current thread. If this call is invoked, and there is no
     * transaction associated with the current thread, a default transaction will be created. Otherwise the current
     * transaction will be used.
     * @param key The key to be set. Needs to be serializable. Can be null.
     * @param sync_timeout Max number of milliseconds to wait for all responses. Note that this needs to be higher
     *                     than <code>lock_acquisition_timeout</code>. 0 means to wait forever for all responses.
     * @param lock_acquisition_timeout Number of milliseonds to wait until a lock becomes available. Needs to be lower
     *                                 than <code>sync_timeout</code>. 0 means to wait forever. <em>0 will block
     *                                 forever in case of deadlocks. Once we have deadlock detection in place,
     *                                 this parameter may be deprecated.</em>
     * @param lock_lease_timeout       Number of milliseonds until a lock is released automatically (if not released
     *                                 before). Not currently used.
     * @param commit                   If true the transaction will be committed after this call if the call was successful.
     * @return Object The previous value associated with the given key, or null if none was associated
     * @exception LockingException Throw when one or more of the members failed acquiring the lock within
     *                             <code>lock_acquisition_timeout</code> milliseconds
     * @exception TimeoutException Thrown when one or more of the members didn't send a response. LockingExceptions
     *                             take precedence over TimeoutExceptions, e.g. if we have both locking and timeout
     *                             exceptions, a LockingException will be thrown.
     */
    public Object remove(Object key, long sync_timeout, 
                         long lock_acquisition_timeout, long lock_lease_timeout,
                         boolean commit) throws LockingException, TimeoutException {
        Data    data;
        Object  retval;
        byte[]  buf;
        Xid     curr_transaction;
        RspList rsps;
        Rsp     rsp;
        
        data=new Data(Data.REMOVE, (Serializable)key);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.remove()", "marshalling failure: " + ex);
            return null;
        }
        retval=get(key);
        curr_transaction=getCurrentTransaction();
        if(curr_transaction == null) {
            Trace.info("TransactionalHashtable.removel()", "no transaction associated with current thread." + 
                       " Will create new transaction with transaction mode=" + Xid.modeToString(transaction_mode));
            try {
                begin(transaction_mode);
            }
            catch(Throwable ex) {
                Trace.error("TransactionalHashtable.remove()", "could not start new transaction: " + ex);
                return null;
            }
            curr_transaction=getCurrentTransaction();
        }
        
        rsps=repl_mgr.send(null,                     // mcast to all members of the group
                           buf,                      // marshalled data
                           true,                     // send synchronously
                           sync_timeout,             // timeout: max time to wait for all responses
                           curr_transaction,         // transaction required
                           null,                     // no lock information needed
                           lock_acquisition_timeout, // lock acquisition timeout not needed
                           lock_lease_timeout,       // lock lease timeout not needed
                           true);                    // don't use locks
        
        // now check results
        if(rsps == null) // should not happen
            Trace.error("TransactionalHashtable.removel()", "RspList of call is null");
        else
            checkResults(rsps); // may throw a LockingException or TimeoutException

        // if commit == true: commit transaction
        if(commit)
            commit(getCurrentTransaction());

        return retval;
    }



    public Object lremove(Object key, Object value) throws LockingException, TimeoutException {
        return remove(key, default_sync_repl_timeout,
                      lock_acquisition_timeout, lock_lease_timeout, auto_commit);
    }






















    /**
     * Replicates the update to all members. Depending on the value of <code>default_sync_repl_timeout</code>
     * the update will be sent synchronously or asynchronously
     */
    public void clear() {
        clear(default_sync_repl, default_sync_repl_timeout);
    }


    /**
     * Replicates the update to all members. Depending on the value of the parameters the update will be
     * synchronous or asynchronous.
     * @param synchronous If true the update will be synchronous, ie. the caller will block until all responses
     *                    have been received. If <code>timeout</code> is 0, we will block indefinitely (until all
     *                    responses have been received), otherwise the call is guaranteed to return after at most
     *                    <code>timeout</code> milliseconds. If false, the call will be asynchronous.
     * @param timeout The number of milliseconds to wait for a synchronous call. 0 means to wait forever.
     *                This parameter is not used if <code>synchronous</code> is false.
     */
    public void clear(boolean synchronous, long timeout) {
        Data   data;
        Object retval;
        byte[] buf;
        
        data=new Data(Data.CLEAR);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.clear()", "marshalling failure: " + ex);
            return;
        }

        repl_mgr.send(null,          // mcast to all members of the group
                      buf,           // marshalled data
                      synchronous,   // send asynchronously
                      timeout,       // timeout (not used on asynchronous send)
                      null,          // no transaction needed
                      null,          // no lock information needed
                      0,             // lock acquisition timeout not needed
                      0,             // lock lease timeout not needed
                      false);        // don't use locks
    }


    /**
     * Replicated the update to all members, and use locks at each member to ensure serializability.
     * When a lock in a member cannot be acquired, a LockingException will be thrown. Typically the caller will
     * then abort the transaction (releasing all locks) and retry.<p>
     * This call can be one of many inside the same transaction, or it may be the only one. In the first case, the
     * caller is responsible to call commit() or rollback() once the transaction is done. In the latter case, the
     * transaction can be committed by setting <code>commit</code> to true.<br>
     * A transaction (Xid) is always associated with the current thread. If this call is invoked, and there is no
     * transaction associated with the current thread, a default transaction will be created. Otherwise the current
     * transaction will be used.
     * @param sync_timeout Max number of milliseconds to wait for all responses. Note that this needs to be higher
     *                     than <code>lock_acquisition_timeout</code>. 0 means to wait forever for all responses.
     * @param lock_acquisition_timeout Number of milliseonds to wait until a lock becomes available. Needs to be lower
     *                                 than <code>sync_timeout</code>. 0 means to wait forever. <em>0 will block
     *                                 forever in case of deadlocks. Once we have deadlock detection in place,
     *                                 this parameter may be deprecated.</em>
     * @param lock_lease_timeout       Number of milliseonds until a lock is released automatically (if not released
     *                                 before). Not currently used.
     * @param commit                   If true the transaction will be committed after this call if the call was successful.
     * @exception LockingException Throw when one or more of the members failed acquiring the lock within
     *                             <code>lock_acquisition_timeout</code> milliseconds
     * @exception TimeoutException Thrown when one or more of the members didn't send a response. LockingExceptions
     *                             take precedence over TimeoutExceptions, e.g. if we have both locking and timeout
     *                             exceptions, a LockingException will be thrown.
     */
    public void clear(long sync_timeout, 
                      long lock_acquisition_timeout, long lock_lease_timeout,
                      boolean commit) throws LockingException, TimeoutException {
        Data    data;
        byte[]  buf;
        Xid     curr_transaction;
        RspList rsps;
        Rsp     rsp;
        
        data=new Data(Data.CLEAR);
        try {
            buf=Util.objectToByteBuffer(data);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.clear()", "marshalling failure: " + ex);
            return;
        }
        curr_transaction=getCurrentTransaction();
        if(curr_transaction == null) {
            Trace.info("TransactionalHashtable.clearl()", "no transaction associated with current thread." + 
                       " Will create new transaction with transaction mode=" + Xid.modeToString(transaction_mode));
            try {
                begin(transaction_mode);
            }
            catch(Throwable ex) {
                Trace.error("TransactionalHashtable.clear()", "could not start new transaction: " + ex);
                return;
            }
            curr_transaction=getCurrentTransaction();
        }
        
        rsps=repl_mgr.send(null,                     // mcast to all members of the group
                           buf,                      // marshalled data
                           true,                     // send synchronously
                           sync_timeout,             // timeout: max time to wait for all responses
                           curr_transaction,         // transaction required
                           null,                     // no lock information needed
                           lock_acquisition_timeout, // lock acquisition timeout not needed
                           lock_lease_timeout,       // lock lease timeout not needed
                           true);                    // don't use locks
        
        // now check results
        if(rsps == null) // should not happen
            Trace.error("TransactionalHashtable.clearl()", "RspList of call is null");
        else
            checkResults(rsps); // may throw a LockingException or TimeoutException

        // if commit == true: commit transaction
        if(commit)
            commit(getCurrentTransaction());
    }



    public void lclear() throws LockingException, TimeoutException {
        clear(default_sync_repl_timeout,
              lock_acquisition_timeout, lock_lease_timeout, auto_commit);
    }















    public boolean containsValue(Object value) {
        return super.containsValue(value); // TODO: implement
    }
    

    public Object clone() {
        return super.clone(); // TODO: implement
    }

    public Set keySet() {
        return super.keySet(); // TODO: implement
    }

    public Collection values() {
        return super.values(); // TODO: implement
    }

    public Set entrySet() {
        return super.entrySet(); // TODO: implement
    }

    /* -------------------------- End of Overridden methods of HashMap ------------------------ */






    /* ---------------------------------- ReplicationReceiver interface ----------------------------- */

    /**
     * Receives an update. Handles the update depending on whether locks are to be used (<code>use_locks</code>):
     * <ol>
     * <li>No locks: simply apply the update to the hashmap
     * <li>Use locks: lock the corresponding resource (e.g. entire table in case of clear(), or individual row in case of
     *                (remove()) and apply the update.If the lock cannot be acquired, throw a LockingException.
     * </ol>
     */
    public Object receive(Xid     transaction,
                          byte[]  buf,
                          byte[]  lock_info,
                          long    lock_acquisition_timeout,
                          long    lock_lease_timeout,
                          boolean use_locks) throws LockingException, UpdateException {
        Data data;

        try {
            data=(Data)Util.objectFromByteBuffer(buf);
            switch(data.getRequestType()) {
            case Data.PUT:
                return handlePut(data.getKey(), data.getValue(),
                                 transaction, lock_acquisition_timeout, lock_lease_timeout, use_locks);
            case Data.PUT_ALL:
                return handlePutAll(data.getMap(), transaction, lock_acquisition_timeout, lock_lease_timeout, use_locks);
            case Data.REMOVE:
                return handleRemove(data.getKey(), transaction, lock_acquisition_timeout, lock_lease_timeout, use_locks);
            case Data.CLEAR:
                return handleClear(transaction, lock_acquisition_timeout, lock_lease_timeout, use_locks);
            default:
                throw new Exception("request type " + data.getRequestType() + " not known");
            }
        }
        catch(Throwable t) {
            Trace.error("TransactionalHashtable.receive()", "exception is " + t);
            return t;
        }
    }

    public void commit(Xid transaction) {
        
    }

    public void rollback(Xid transaction) {
        
    }
        
    /* ------------------------------ End of ReplicationReceiver interface -------------------------- */





    /* ------------------------------------ MessageListener interface ------------------------------- */

    public void receive(Message msg) {
        ;
    }


    /**
     * TODO: use read lock on entire hashmap while making copy
     */
    public byte[] getState() {
	HashMap   copy=new HashMap();
        Map.Entry entry;
        
        for(Iterator it=entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            copy.put(entry.getKey(), entry.getValue());
        }

        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.getState()", "exception marshalling state: " + ex);
            return null;
        }
    }


    /**
     * TODO: use write lock on entire hashmap to set state
     */
    public void setState(byte[] state) {
        HashMap   new_copy;
        Map.Entry entry;

        try {
            new_copy=(HashMap)Util.objectFromByteBuffer(state);
            if(new_copy == null)
                return;
        }
        catch(Throwable ex) {
            Trace.error("TransactionalHashtable.setState()", "exception unmarshalling state: " + ex);
            return;
        }

	super.clear(); // remove all elements
        
	for(Iterator it=new_copy.entrySet().iterator(); it.hasNext();) {
	    entry=(Map.Entry)it.next();
	    super.put(entry.getKey(), entry.getValue());
	}
        if(Trace.trace)
            Trace.info("TransactionalHashtable.setState()", "hashmap has " + size() + " items");
    }

    /* --------------------------------- End of MessageListener interface --------------------------- */



    /**
     * Leaves the group. The instance is unusable after this call, ie. a new instance should be created.
     * Behavior is undefined if the instance is still used after this call.
     */
    public void stop() {
        if(repl_mgr != null)
            repl_mgr.stop();
    }


    public void setMembershipListener(MembershipListener ml) {
        if(ml == null) return;
        if(repl_mgr == null)
            Trace.error("TransactionalHashtable.setMembershipListener()", "ReplicationManager is null");
        else
            repl_mgr.setMembershipListener(ml);
    }


    public boolean isDefaultSyncRepl() {
        return default_sync_repl;
    }

    /**
     * Sets the default replication mode. This will be used if the methods inherited from HashMap are used.
     * However, if one of the methods provided by TransactionalHashtable are used, they will override the default mode.
     */
    public void setDefaultSyncRepl(boolean b) {
        default_sync_repl=b;
    }

    public long getDefaultSyncReplTimeout() {
        return default_sync_repl_timeout;
    }

    public void setDefaultSyncReplTimeout(long timeout) {
        default_sync_repl_timeout=timeout;
    }

    public boolean getAutoCommit() {
        return auto_commit;
    }

    public void setAutoCommit(boolean b) {
        auto_commit=b;
    }

    public long getLockAcquisitionTimeout() {
        return lock_acquisition_timeout;
    }
    
    public void setLockAcquisitionTimeout(long l) {
        lock_acquisition_timeout=l;
    }
    
    public long getLockLeaseTimeout() {
        return lock_lease_timeout;
    }

    public void setLockLeaseTimeout(long l) {
        lock_lease_timeout=l;
    }

    public int getTransactionMode() {
        return transaction_mode;
    }

    public void setTransactionMode(int m) {
        transaction_mode=m;
    }


    /**
     * Starts a new transaction and associates it with the current thread. Reuses the transaction if the
     * current thread already has a transaction.
     */
    public void begin() throws Exception {
        begin(Xid.DIRTY_READS);
    }

    /**
     * Starts a new transaction and associates it with the current thread. Reuses the transaction if the
     * current thread already has a transaction.
     * @param transaction_mode Mode in which the transaction should run. Possible values are Xid.DIRTY_READS,
     *                         Xid.READ_COMMITTED, Xid.REPEATABLE_READ and Xid.SERIALIZABLE
     */
    public void begin(int transaction_mode) throws Exception {
        Xid         xid;
        Object      m=thread_local.get();

        if(m == null) {
            m=new HashMap();
            thread_local.set(m);
        }
        else if(!(m instanceof Map)) {
            Trace.warn("ReplicationManager.begin()", "thread local data was not a hashmap (was " +
                       m + "); setting data to be a hashmap");
            m=new HashMap();
            thread_local.set(m);
        }
        
        if((xid=(Xid)((Map)m).get(Xid.XID)) != null) {
            Trace.warn("ReplicationManager.begin()", "transaction already present (will be reused): " + xid);
            return;
        }
        else {
            xid=repl_mgr.begin(transaction_mode);
            ((Map)m).put(Xid.XID, xid);
        }
    }


    /**
     * Commits all modifications done in the current transaction (kept in temporary storage)
     * to the hashtable. Releases all locks acquired by the current transaction.
     */
    public void commit() {
        Xid curr_tx=getCurrentTransaction();
        if(curr_tx == null)
            Trace.error("TransactionalHashtable.commit()", "no transaction associated with current thread");
        else {
            repl_mgr.commit(curr_tx);
            ((Map)thread_local.get()).remove(Xid.XID);
        }
    }

    
    /**
     * Discards all changes done within the current transaction. Releases all locks acquired
     * by the current transaction.
     */
    public void rollback() {
        Xid curr_tx=getCurrentTransaction();
        if(curr_tx == null)
            Trace.error("TransactionalHashtable.rollback()", "no transaction associated with current thread");
        else {
            repl_mgr.rollback(curr_tx);
            ((Map)thread_local.get()).remove(Xid.XID);
        }
    }



    /**
     * Returns the transaction associated with the current thread.
     * @return Xid The current transaction. Null if no transaction is associated.
     */
    public static Xid getCurrentTransaction() {
        Xid         xid;
        Object      m=thread_local.get();
        
        if(m == null || !(m instanceof Map))
            return null;        
        return (Xid)((Map)m).get(Xid.XID);
    }








    /**
     * Class used to transport updates to all replicas
     */
    public static class Data implements Externalizable {
        public static final int PUT     = 1;
        public static final int PUT_ALL = 2;
        public static final int REMOVE  = 3;
        public static final int CLEAR   = 4;        

        int          request=0;
        Serializable key=null;   // for PUT, REMOVE
        Serializable value=null; // for PUT
        Map          map=null;   // for PUT_ALL


        /** Used by externalization */
        public Data() {
            ;
        }


        public Data(int request_type) {
            request=request_type;
        }

        public Data(int request_type, Serializable key, Serializable value) {
            this(request_type);
            this.key=key;
            this.value=value;
        }

        public Data(int request_type, Serializable key) {
            this(request_type);
            this.key=key;
        }

        public Data(int request_type, Map map) {
            this(request_type);
            this.map=map;
        }


        public Data(int request_type, Serializable key, Serializable value, Map map) {
            this(request_type, key, value);
            this.map=map;
        }


        public int getRequestType() {
            return request;
        }

        public Serializable getKey() {
            return key;
        }

        public Serializable getValue() {
            return value;
        }

        public Map getMap() {
            return map;
        }


        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(requestToString(request));
            switch(request) {
            case PUT:
            case REMOVE:
                sb.append(" key=").append(key).append(", value=").append(value);
                break;
            case PUT_ALL:
                sb.append(" map=").append(map.size()).append(" items");
                break;
            case CLEAR:
                break;
            default:
                break;
            }
            return sb.toString();
        }


        public String requestToString(int r) {
            switch(r) {
            case PUT:     return "PUT";
            case PUT_ALL: return "PUT_ALL";
            case REMOVE:  return "REMOVE";
            case CLEAR:   return "CLEAR";
            default:      return "<unknown>";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(request);
            out.writeObject(key);
            out.writeObject(value);
            out.writeObject(map);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            request=in.readInt();
            key=(Serializable)in.readObject();
            value=(Serializable)in.readObject();
            map=(Map)in.readObject();
        }

    }





    /* ----------------------------------------- Private methods ------------------------------------ */

    protected void initChannel(String groupname, String properties, long state_timeout) throws Exception {
        this.groupname=groupname;
        this.properties=properties;
        this.state_timeout=state_timeout;

        Trace.init();
        channel=new JChannel(properties);
        channel.setOpt(Channel.GET_STATE_EVENTS, new Boolean(true));
        channel.connect(groupname);
        repl_mgr=new ReplicationManager(channel, this, null, this);

        if(channel.getState(null, state_timeout))
            Trace.info("TransactionalHashtable.TransactionalHashtable()", "state was retrieved successfully");
        else
            Trace.info("TransactionalHashtable.TransactionalHashtable()", "state could not be retrieved (first member)");
    }

    
    protected Object handlePut(Serializable key, Serializable value, Xid transaction,
                               long lock_acquisition_timeout, long lock_lease_timeout,
                               boolean use_locks) throws LockingException, UpdateException {
        if(Trace.trace) {
            StringBuffer sb=new StringBuffer();
            sb.append("key=").append(key).append(", value=").append(value).append(", use_locks=").append(use_locks);
            if(use_locks) {
                sb.append(", transaction=").append(transaction);
                sb.append(", lock_acquisition_timeout=").append(lock_acquisition_timeout);
                sb.append(", lock_lease_timeout=").append(lock_lease_timeout);
            }
            Trace.info("TransactionalHashtable.handlePut()", sb.toString());
        }

        if(!use_locks) {
            try {
                return super.put(key, value);
            }
            catch(Throwable ex) {
                throw new UpdateException("TransactionalHashtable.handlePut(): exception is " + ex);
            }
        }

        return null; // TODO: implement
    }


    protected Object handlePutAll(Map map, Xid transaction,
                                  long lock_acquisition_timeout, long lock_lease_timeout,
                                  boolean use_locks) throws LockingException, UpdateException {
        return null;
    }


    protected Object handleRemove(Serializable key, Xid transaction,
                                  long lock_acquisition_timeout, long lock_lease_timeout,
                                  boolean use_locks) throws LockingException, UpdateException {
        return null;
    }

    protected Object handleClear(Xid transaction, long lock_acquisition_timeout, long lock_lease_timeout,
                                 boolean use_locks) throws LockingException, UpdateException {
        return null;
    }


    /**
     * Checks whether responses from members contain exceptions or timeouts. Throws an exception
     * if that is the case
     */
    protected void checkResults(RspList rsps) throws LockingException, TimeoutException {
        Map              ml=null;
        List             ll=null;
        LockingException l=null;
        TimeoutException t=null;
        Rsp              rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);

            // check for exceptions
            if(rsp.getValue() != null && rsp.getValue() instanceof Throwable) {
                if(l == null)
                    l=new LockingException(ml=new HashMap());
                ml.put(rsp.getSender(), rsp.getValue());
            }

            // check for timeouts
            if(rsp.wasReceived() == false) {
                if(t == null)
                    t=new TimeoutException(ll=new ArrayList());
                ll.add(rsp.getSender());
            }
        }
        if(l != null)
            throw l;
        if(t != null)
            throw t;        
    }


    /* -------------------------------------- End of Private methods --------------------------------- */


    // FIXME: remove
    public static void main(String args[]) {
        TransactionalHashtable th;
        String                 val;

        if(args.length != 2) {
            System.err.println("TransactionalHashtable <key> <val>");
            return;
        }

        try {
            th=new TransactionalHashtable("bla", "file:/home/bela/state_transfer.xml", 3000);
            System.out.println("-- TransactionalHashtable created");
            System.out.println("-- contents:\n" + dump(th));
            th.put(args[0], args[1], true, 5000);
        }
        catch(Throwable ex) {
            ex.printStackTrace();
        }
    }

    
    static String dump(Map m) {
        StringBuffer sb=new StringBuffer();
        Map.Entry    entry;

        for(Iterator it=m.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(" --> ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

}
