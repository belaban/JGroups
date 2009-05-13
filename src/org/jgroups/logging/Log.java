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

    void debug(String msg);
    void debug(String msg, Throwable throwable);
    void error(String msg);
    void error(String msg, Throwable throwable);
    void fatal(String msg);
    void fatal(String msg, Throwable throwable);
    void info(String msg);
    void info(String msg, Throwable throwable);
    void trace(Object msg);
    void trace(Object msg, Throwable throwable);
    void trace(String msg);
    void trace(String msg, Throwable throwable);
    void warn(String msg);
    void warn(String msg, Throwable throwable);
}
