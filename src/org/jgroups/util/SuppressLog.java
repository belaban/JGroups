package org.jgroups.util;

import org.jgroups.logging.Log;

import java.text.MessageFormat;

/**
 * Log (using {@link SuppressCache}) which suppresses (certain) messages from the same member for a given time
 * @author Bela Ban
 * @since  3.2
 */
public class SuppressLog<T> {
    protected final Log              log;
    protected final SuppressCache<T> cache;
    protected final String           message_format;
    protected final String           suppress_format;

    public static enum Level {warn,error};

    public SuppressLog(Log log, String message_key, String suppress_msg) {
        this.log=log;
        cache=new SuppressCache<T>();
        message_format=Util.getMessage(message_key);
        suppress_format=Util.getMessage(suppress_msg); // "(received {3} identical messages from {2} in the last {4} ms)"
    }

    public SuppressCache<T> getCache()               {return cache;}

    /**
     * Logs a message from a given member if is hasn't been logged for timeout ms
     * @param level The level, either warn or error
     * @param key The key into the SuppressCache
     * @param timeout The timeout
     * @param args The arguments to the message key
     */
    public void log(Level level, T key, long timeout, Object ... args) {
        SuppressCache.Value val=cache.putIfAbsent(key, timeout);
        if(val == null) // key is present and hasn't expired
            return;

        String message=val.count() == 1? MessageFormat.format(message_format, args) :
          MessageFormat.format(message_format, args) + " " + MessageFormat.format(suppress_format, val.count(), key, val.age());

        if(level == Level.error)
            log.error(message);
        else
            log.warn(message);
    }


    public void removeExpired(long timeout) {cache.removeExpired(timeout);}

}
