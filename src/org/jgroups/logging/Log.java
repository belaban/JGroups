package org.jgroups.logging;

/**
 * Simple logging wrapper for log4j or JDK logging. Code originally copied from Infinispan
 *
 * @author Manik Surtani
 * @author Bela Ban
 * @since 2.8
 */
public interface Log {
    boolean isFatalEnabled();
    boolean isErrorEnabled();
    boolean isWarnEnabled();
    boolean isInfoEnabled();
    boolean isDebugEnabled();
    boolean isTraceEnabled();



    void fatal(String msg);
    void fatal(String format, Object ... args);
    void fatal(String msg, Throwable throwable);

    void error(String msg);
    void error(String format, Object ... args);
    void error(String msg, Throwable throwable);

    void warn(String msg);
    void warn(String format, Object ... args);
    void warn(String msg, Throwable throwable);

    void info(String msg);
    void info(String format, Object ... args);

    void debug(String msg);
    void debug(String format, Object ... args);
    void debug(String msg, Throwable throwable);

    void trace(Object obj);
    void trace(String msg);
    void trace(String format, Object ... args);
    void trace(String msg, Throwable throwable);



    // Advanced methods

    /**
     * Sets the level of a logger. This method is used to dynamically change the logging level of a running system,
     * e.g. via JMX. The appender of a level needs to exist.
     * @param level The new level. Valid values are "fatal", "error", "warn", "info", "debug", "trace"
     * (capitalization not relevant)
     */
    void setLevel(String level);

    String getLevel();
}
