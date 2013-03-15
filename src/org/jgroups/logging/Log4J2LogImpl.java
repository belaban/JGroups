package org.jgroups.logging;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.simple.SimpleLogger;

/**
 * Logger that delivers messages to a Log4J2 logger
 * 
 * @author Bela Ban
 * @since  3.3
 */
public class Log4J2LogImpl implements Log {
    protected final Logger logger;

    protected static final Level[] levels={Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL};


    public Log4J2LogImpl(String category) {
        logger=LogManager.getFormatterLogger(category);
    }

    public Log4J2LogImpl(Class<?> category) {
        logger = LogManager.getFormatterLogger(category);
    }

    public boolean isFatalEnabled() {return logger.isFatalEnabled();}
    public boolean isErrorEnabled() {return logger.isErrorEnabled();}
    public boolean isWarnEnabled()  {return logger.isWarnEnabled();}
    public boolean isInfoEnabled()  {return logger.isInfoEnabled();}
    public boolean isDebugEnabled() {return logger.isDebugEnabled();}
    public boolean isTraceEnabled() {return logger.isTraceEnabled();}



    public void trace(Object msg) {
        logger.trace(msg);
    }

    public void trace(String msg) {
        logger.trace(msg);
    }

    public void trace(String msg, Object... args) {
        logger.trace(msg, args);
    }

    public void trace(String msg, Throwable throwable) {
        logger.trace(msg, throwable);
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void debug(String msg, Object... args) {
        logger.debug(msg, args);
    }

    public void debug(String msg, Throwable throwable) {
        logger.debug(msg, throwable);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void info(String msg, Object... args) {
        logger.info(msg, args);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void warn(String msg, Object... args) {
        logger.warn(msg, args);
    }

    public void warn(String msg, Throwable throwable) {
        logger.warn(msg, throwable);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public void error(String format, Object... args) {
        logger.error(format, args);
    }

    public void error(String msg, Throwable throwable) {
        logger.error(msg, throwable);
    }

    public void fatal(String msg) {
        logger.fatal(msg);
    }

    public void fatal(String msg, Object... args) {
        logger.fatal(msg, args);
    }

    public void fatal(String msg, Throwable throwable) {
        logger.fatal(msg, throwable);
    }






    public String getLevel() {
        for(Level level: levels)
            if(logger.isEnabled(level))
                return level.toString();
        return "n/a";
    }

    public void setLevel(String level) {
        Level new_level=strToLevel(level);
        if(new_level == null)
            return;
        if(logger instanceof org.apache.logging.log4j.core.Logger)
            ((org.apache.logging.log4j.core.Logger)logger).setLevel(new_level);
        else if(logger instanceof SimpleLogger)
            ((SimpleLogger)logger).setLevel(new_level);
    }

    private static Level strToLevel(String level) {
        if (level == null)
            return null;
        level = level.toLowerCase().trim();
        if (level.equals("fatal"))
            return Level.FATAL;
        if (level.equals("error"))
            return Level.ERROR;
        if (level.equals("warn"))
            return Level.WARN;
        if (level.equals("warning"))
            return Level.WARN;
        if (level.equals("info"))
            return Level.INFO;
        if (level.equals("debug"))
            return Level.DEBUG;
        if (level.equals("trace"))
            return Level.TRACE;
        return null;
    }
}
