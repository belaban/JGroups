package org.jgroups.blocks;

import org.jgroups.util.Streamable;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

/**
 * Cache which allows for replication factors <em>per data items</em>; the factor determines how many replicas
 * of a key/value we create across the cluster.<br/>
 * See doc/design/ReplCache.txt for details.
 * @author Bela Ban
 * @version $Id: ReplCache.java,v 1.2 2008/12/23 16:52:25 belaban Exp $
 */
public class ReplCache<K,V> {
    private final ConcurrentMap<K,V> cache=new ConcurrentHashMap<K,V>();

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
    public void put(K key, V val, short repl_factor, long timeout) {

    }

    /**
     * Returns the value associated with key
     * @param key The key, has to be serializable
     * @return The value associated with key, or null
     */
    public V get(K key) {
        return null;
    }

    /**
     * Removes key in all nodes in the cluster, both from their local hashmaps and L1 caches
     * @param key The key, needs to be serializable 
     */
    public void remove(K key) {
        
    }

    private static class Message<K,V> implements Streamable {
        static enum Type {PUT, GET, REMOVE};

        private Type type;
        private K key;
        private V val;
        private short repl_factor=-1; // default is replicate everywhere
        private long timeout=30000;

        private Message(Type type, K key, V val, short repl_factor, long timeout) {
            this.type=type;
            this.key=key;
            this.val=val;
            this.repl_factor=repl_factor;
            this.timeout=timeout;
        }

        private Message(Type type, K key) {
            this.type=type;
            this.key=key;
        }

        public static <K,V> Message<K,V> createPutMessage(K key, V val, short repl_factor, long timeout) {
            return new Message<K,V>(Type.PUT, key, val, repl_factor, timeout);
        }

        public static <K,V> Message<K,V> createGetMessage(K key) {
            return new Message<K,V>(Type.GET, key);
        }

        public static <K,V> Message<K,V> createRemoveMessage(K key) {
            return new Message<K,V>(Type.REMOVE, key);
        }


        public String toString() {
            return "type=" + type +
                    ", key=" + key +
                    ", val=" + val +
                    ", repl_factor=" + repl_factor +
                    ", timeout=" + timeout;
        }


        public void writeTo(DataOutputStream out) throws IOException {
            switch(type) {
                case PUT:
                    out.writeByte(1);
                    break;
                case GET:
                    out.writeByte(2);
                    break;
                case REMOVE:
                    out.writeByte(3);
                    break;
                default:
                    throw new IllegalStateException("type " + type + " is not handled");
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            byte tmp=in.readByte();
            switch(tmp) {
                case 1:
                    type=Type.PUT;
                    break;
                case 2:
                    type=Type.GET;
                    break;
                case 3:
                    type=Type.REMOVE;
                    break;
                default:
                    throw new IllegalStateException("type " + tmp + " is not handled");
            }
        }
    }
}
