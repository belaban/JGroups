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

    @Override
    public boolean isFatalEnabled() {return logger.isFatalEnabled();}
    @Override
    public boolean isErrorEnabled() {return logger.isErrorEnabled();}
    @Override
    public boolean isWarnEnabled()  {return logger.isWarnEnabled();}
    @Override
    public boolean isInfoEnabled()  {return logger.isInfoEnabled();}
    @Override
    public boolean isDebugEnabled() {return logger.isDebugEnabled();}
    @Override
    public boolean isTraceEnabled() {return logger.isTraceEnabled();}



    @Override
    public void trace(Object msg) {
        logger.trace(msg);
    }

    @Override
    public void trace(String msg) {
        logger.trace(msg);
    }

    @Override
    public void trace(String format, Object... args) {
        logger.trace(format, args);
    }

    @Override
    public void trace(String msg, Throwable throwable) {
        logger.trace(msg, throwable);
    }

    @Override
    public void debug(String msg) {
        logger.debug(msg);
    }

    @Override
    public void debug(String format, Object... args) {
        logger.debug(format, args);
    }

    @Override
    public void debug(String msg, Throwable throwable) {
        logger.debug(msg, throwable);
    }

    @Override
    public void info(String msg) {
        logger.info(msg);
    }

    @Override
    public void info(String format, Object... args) {
        logger.info(format, args);
    }

    @Override
    public void warn(String msg) {
        logger.warn(msg);
    }

    @Override
    public void warn(String format, Object... args) {
        logger.warn(format, args);
    }

    @Override
    public void warn(String msg, Throwable throwable) {
        logger.warn(msg, throwable);
    }

    @Override
    public void error(String msg) {
        logger.error(msg);
    }

    @Override
    public void error(String format, Object... args) {
        logger.error(format, args);
    }

    @Override
    public void error(String msg, Throwable throwable) {
        logger.error(msg, throwable);
    }

    @Override
    public void fatal(String msg) {
        logger.fatal(msg);
    }

    @Override
    public void fatal(String format, Object... args) {
        logger.fatal(format, args);
    }

    @Override
    public void fatal(String msg, Throwable throwable) {
        logger.fatal(msg, throwable);
    }






    @Override
    public String getLevel() {
        for(Level level: levels)
            if(logger.isEnabled(level))
                return level.toString();
        return "n/a";
    }

    @Override
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
