package org.jgroups.logging;

import java.util.Arrays;
import java.util.IllegalFormatException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Logger that delivers messages to a JDK logger
 * @author Manik Surtani
 * @author Bela Ban
 * @since 2.8
 */
public class JDKLogImpl implements Log {
    protected final Logger logger;

    public JDKLogImpl(String category) {
        logger=Logger.getLogger(category);
    }

    public JDKLogImpl(Class<?> clazz) {
        logger=Logger.getLogger(clazz.getName()); // fix for https://jira.jboss.org/browse/JGRP-1224
    }


    private void log(Level lv, String msg) {
        log(lv,msg,null);
    }

    /**
     * To correctly attribute the source class/method name to that of the JGroups class,
     * we can't let JDK compute that. Instead, we do it on our own.
     */
    private void log(Level lv, String msg, Throwable e) {
        if (logger.isLoggable(lv)) {
            LogRecord r = new LogRecord(lv, msg);
            r.setThrown(e);

            // find the nearest ancestor that doesn't belong to JDKLogImpl
            for (StackTraceElement frame : new Exception().getStackTrace()) {
                if (!frame.getClassName().equals(THIS_CLASS_NAME)) {
                    r.setSourceClassName(frame.getClassName());
                    r.setSourceMethodName(frame.getMethodName());
                    break;
                }
            }

            logger.log(r);
        }
    }
    
    @Override
    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINER);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public boolean isFatalEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void trace(String msg) {
        log(Level.FINER, msg);
    }

    @Override
    public void trace(String format, Object... args) {
        if(isTraceEnabled())
            log(Level.FINER, format(format, args));
    }

    @Override
    public void trace(Object msg) {
        log(Level.FINER, msg.toString());
    }

    @Override
    public void trace(String msg, Throwable t) {
        log(Level.FINER, msg, t);
    }

    @Override
    public void debug(String msg) {
        log(Level.FINE, msg);
    }

    @Override
    public void debug(String format, Object... args) {
        if(isDebugEnabled())
            log(Level.FINE, format(format, args));
    }

    @Override
    public void debug(String msg, Throwable t) {
        log(Level.FINE, msg, t);
    }

    @Override
    public void info(String msg) {
        log(Level.INFO, msg);
    }

    @Override
    public void info(String format, Object... args) {
        if(isInfoEnabled())
            log(Level.INFO, format(format, args));
    }

    @Override
    public void warn(String msg) {
        log(Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object... args) {
        if(isWarnEnabled())
            log(Level.WARNING, format(format, args));
    }

    @Override
    public void warn(String msg, Throwable t) {
        log(Level.WARNING, msg, t);
    }

    @Override
    public void error(String msg) {
        log(Level.SEVERE, msg);
    }

    @Override
    public void error(String format, Object... args) {
        if(isErrorEnabled())
            log(Level.SEVERE, format(format, args));
    }

    @Override
    public void error(String msg, Throwable t) {
        log(Level.SEVERE, msg, t);
    }

    @Override
    public void fatal(String msg) {
        log(Level.SEVERE, msg);
    }

    @Override
    public void fatal(String format, Object... args) {
        if(isFatalEnabled())
            log(Level.SEVERE, format(format, args));
    }

    @Override
    public void fatal(String msg, Throwable t) {
        log(Level.SEVERE, msg, t);
    }

    @Override
    public String getLevel() {
        Level level=logger.getLevel();
        return level != null? level.toString() : "off";
    }

    @Override
    public void setLevel(String level) {
        Level new_level=strToLevel(level);
        if(new_level != null)
            logger.setLevel(new_level);
    }

    protected String format(String format, Object... args) {
        try {
            return String.format(format, args);
        }
        catch(IllegalFormatException ex) {
            error("Illegal format string \"" + format + "\", args=" + Arrays.toString(args));
        }
        catch(Throwable t) {
            error("Failure formatting string: format string=" + format + ", args=" + Arrays.toString(args));
        }
        return format;
    }

    protected static Level strToLevel(String level) {
        if(level == null) return null;
        level=level.toLowerCase().trim();
        if(level.equals("fatal"))   return Level.SEVERE;
        if(level.equals("error"))   return Level.SEVERE;
        if(level.equals("warn"))    return Level.WARNING;
        if(level.equals("warning")) return Level.WARNING;
        if(level.equals("info"))    return Level.INFO;
        if(level.equals("debug"))   return Level.FINE;
        if(level.equals("trace"))   return Level.FINER;
        return null;
    }

    private static final String THIS_CLASS_NAME = JDKLogImpl.class.getName();
}
