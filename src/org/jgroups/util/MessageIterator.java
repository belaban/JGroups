package org.jgroups.util;

import org.jgroups.Message;

import java.util.Iterator;

/**
 * @author Bela Ban
 * @since  4.2.0
 */
public interface MessageIterator extends Iterator<Message> {
    /** Replaces the message at the current index with msg */
    void replace(Message msg);
}
