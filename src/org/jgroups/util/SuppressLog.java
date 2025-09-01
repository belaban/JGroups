package org.jgroups.util;

import org.jgroups.logging.Log;

import java.util.concurrent.TimeUnit;

/**
 * Log (using {@link SuppressCache}) which suppresses (certain) messages from the same member for a given time
 * @param <T> T
 * @author Bela Ban
 * @since  3.2
 */
public class SuppressLog<T> {
    protected final Log              log;
    protected final SuppressCache<T> cache;
    protected final String           message_format;
    // "(%d identical messages in the last %s)"
    protected final String           suppress_format=Util.getMessage("SuppressMsg");

    public enum Level {error,warn,trace};

    public SuppressLog(Log log, String message_key) {
        this.log=log;
        cache=new SuppressCache<>();
        message_format=Util.getMessage(message_key);
    }

    public SuppressLog(Log log) {
        this(log, null);
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
        String message=createMessage(val, message_format, args);
        log(level, message);
    }

    public void log(Level level, T key, long timeout, String format, Object... args) {
        SuppressCache.Value val=cache.putIfAbsent(key, timeout);
        if(val == null) // key is present and hasn't expired
            return;
        String message=createMessage(val, format, args);
        log(level, message);
    }

    public void error(T key, long timeout, Object ... args) {
        log(Level.error, key, timeout, args);
    }

    public void warn(T key, long timeout, Object ... args) {
        log(Level.warn, key, timeout, args);
    }

    public void trace(T key, long timeout, Object ... args) {
        log(Level.trace, key, timeout, args);
    }

    public void error(T key, long timeout, String format, Object...args) {
        log(Level.error, key, timeout, format, args);
    }

    public void warn(T key, long timeout, String format, Object...args) {
        log(Level.warn, key, timeout, format, args);
    }

    public void trace(T key, long timeout, String format, Object...args) {
        log(Level.trace, key, timeout, format, args);
    }

    protected String createMessage(SuppressCache.Value val, String format, Object... args) {
        return val.count() == 1? String.format(format, args) :
          String.format(format, args) + " "
            + String.format(suppress_format, val.count(), Util.printTime(val.age(), TimeUnit.MILLISECONDS));
    }

    protected void log(Level level, String message) {
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
