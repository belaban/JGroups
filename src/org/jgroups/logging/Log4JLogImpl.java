package org.jgroups.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Logger that delivers messages to a Log4J logger
 * @author Manik Surtani
 * @author Bela Ban
 * @since 2.8
 */
public class Log4JLogImpl implements Log {
    private final Logger logger;


    public Log4JLogImpl(String category) {
        logger=Logger.getLogger(category);
    }

    public Log4JLogImpl(Class category) {
        logger=Logger.getLogger(category);
    }

    public boolean isFatalEnabled() {
        return logger.isEnabledFor(Level.FATAL);
    }

    public boolean isErrorEnabled() {
        return logger.isEnabledFor(Level.ERROR);
    }

    public boolean isWarnEnabled() {
        return logger.isEnabledFor(Level.WARN);
    }

    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void debug(String msg, Throwable throwable) {
        logger.debug(msg, throwable);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public void error(String msg, Throwable throwable) {
        logger.error(msg, throwable);
    }

    public void fatal(String msg) {
        logger.fatal(msg);
    }

    public void fatal(String msg, Throwable throwable) {
        logger.fatal(msg, throwable);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void info(String msg, Throwable throwable) {
        logger.info(msg, throwable);
    }

    public void trace(Object msg) {
        logger.trace(msg);
    }

    public void trace(Object msg, Throwable throwable) {
        logger.trace(msg, throwable);
    }

    public void trace(String msg) {
        logger.trace(msg);
    }

    public void trace(String msg, Throwable throwable) {
        logger.trace(msg, throwable);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void warn(String msg, Throwable throwable) {
        logger.warn(msg, throwable);
    }
}
