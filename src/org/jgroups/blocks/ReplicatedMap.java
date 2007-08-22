package org.jgroups.blocks;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: ReplicatedMap.java,v 1.1 2007/08/22 08:50:07 belaban Exp $
 */
public interface ReplicatedMap<K extends Serializable, V extends Serializable> extends Map<K, V> {
    V _put(K key, V value);

    void _putAll(Map<? extends K, ? extends V> map);

    void _clear();

    V _remove(Object key);
}
