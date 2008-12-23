package org.jgroups.blocks;

/**
 * Cache which allows for replication factors <em>per data items</em>; the factor determines how many replicas
 * of a key/value we create across the cluster.<br/>
 * See doc/design/ReplCache.txt for details.
 * @author Bela Ban
 * @version $Id: ReplCache.java,v 1.1 2008/12/23 16:30:12 belaban Exp $
 */
public class ReplCache {

    /**
     * Places a key/value pair into one or several nodes in the cluster.
     * @param key The key, needs to be serializable
     * @param val The value, needs to be serializable
     * @param repl_factor Number of replicas.
     * <ul>
     * <li>-1: create key/val in all the nodes in the cluster
     * <li>0: create key/val only in one node in the cluster, picked by computing the consistent hash of KEY
     * <li>K &gt; 0: create key/val in those nodes in the cluster which match the consistent hashes created for KEY
     * </ul> 
     * @param timeout Expiration time for key/value.
     * <ul>
     * <li>-1: don't cache at all in the L1 cache
     * <li>0: cache forever, until removed or evicted because we need space for newer elements
     * <li>&gt; 0: number of milliseconds to keep an idle element in the cache. An element is idle when not accessed.
     * </ul>
     */
    public void put(Object key, Object val, short repl_factor, long timeout) {

    }

    /**
     * Returns the value associated with key
     * @param key The key, has to be serializable
     * @return The value associated with key, or null
     */
    public Object get(Object key) {
        return null;
    }

    /**
     * Removes key in all nodes in the cluster, both from their local hashmaps and L1 caches
     * @param key The key, needs to be serializable 
     */
    public void remove(Object key) {
        
    }
}
