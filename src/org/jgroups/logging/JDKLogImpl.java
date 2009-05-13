package org.jgroups.logging;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logger that delivers messages to a JDK logger
 * @author Manik Surtani
 * @author Bela Ban
 * @since 2.8
 */
public class JDKLogImpl implements Log {
    private final Logger logger;

    public JDKLogImpl(String category) {
        logger=Logger.getLogger(category);
    }

    public JDKLogImpl(Class category) {
        logger=Logger.getLogger(category.toString());
    }

    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINER);
    }

    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public boolean isFatalEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public void trace(String msg) {
        logger.log(Level.FINER, msg);
    }

    public void trace(Object msg) {
        logger.log(Level.FINER, msg.toString());
    }

    public void debug(String msg) {
        logger.log(Level.FINE, msg);
    }

    public void info(String msg) {
        logger.log(Level.INFO, msg);
    }

    public void warn(String msg) {
        logger.log(Level.WARNING, msg);
    }

    public void error(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    public void fatal(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    public void trace(Object msg, Throwable t) {
        logger.log(Level.FINER, msg.toString(), t);
    }

    public void trace(String msg, Throwable t) {
        logger.log(Level.FINER, msg, t);
    }

    public void debug(String msg, Throwable t) {
        logger.log(Level.FINE, msg, t);
    }

    public void info(String msg, Throwable t) {
        logger.log(Level.INFO, msg, t);
    }

    public void warn(String msg, Throwable t) {
        logger.log(Level.WARNING, msg, t);
    }

    public void error(String msg, Throwable t) {
        logger.log(Level.SEVERE, msg, t);
    }

    public void fatal(String msg, Throwable t) {
        logger.log(Level.SEVERE, msg, t);
    }


}
