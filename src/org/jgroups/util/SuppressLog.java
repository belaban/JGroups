package org.jgroups.util;

import org.jgroups.logging.Log;

import java.util.concurrent.TimeUnit;

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

    public enum Level {error,warn,trace};

    public SuppressLog(Log log, String message_key) {
        this.log=log;
        cache=new SuppressCache<>();
        message_format=Util.getMessage(message_key);
        suppress_format=Util.getMessage("SuppressMsg"); // "(%d identical messages in the last %s)"
    }

    public SuppressCache<T> getCache()               {return cache;}

    /**
     * Logs a message from a given member if is hasn't been logged for timeout ms
     * @param level The level, either warn or error
     * @param key The key into the SuppressCache, e.g. a member address or other topic ("thread_pool_full")
     * @param timeout The timeout
     * @param args The arguments to the message key
     */
    public void log(Level level, T key, long timeout, Object ... args) {
        SuppressCache.Value val=cache.putIfAbsent(key, timeout);
        if(val == null) // key is present and hasn't expired
            return;

        String message=val.count() == 1? String.format(message_format, args) :
          String.format(message_format, args) + " "
            + String.format(suppress_format, val.count(), Util.printTime(val.age(), TimeUnit.MILLISECONDS));

        switch(level) {
            case error:
                log.error(message);
                break;
            case warn:
                log.warn(message);
                break;
            case trace:
                log.trace(message);
                break;
        }
    }


    public void removeExpired(long timeout) {cache.removeExpired(timeout);}

}
