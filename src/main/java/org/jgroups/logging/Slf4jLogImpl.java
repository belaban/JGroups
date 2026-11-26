package org.jgroups.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Logger that delivers messages to a SLF4J logger
 *
 * @author Konstantin Gusarov
 * @since 4.0.0
 */
public class Slf4jLogImpl implements Log {
    private static final Map<Function<Logger, Boolean>, String> LEVELS = new LinkedHashMap<>();
    private static final Locale LOCALE = Locale.getDefault();

    static {
        LEVELS.put(Logger::isErrorEnabled, "ERROR");
        LEVELS.put(Logger::isWarnEnabled, "WARN");
        LEVELS.put(Logger::isInfoEnabled, "INFO");
        LEVELS.put(Logger::isDebugEnabled, "DEBUG");
        LEVELS.put(Logger::isTraceEnabled, "TRACE");
    }

    private final Logger logger;
    private final Locale locale;

    public Slf4jLogImpl(final Class<?> clazz) {
        this(LOCALE, LoggerFactory.getLogger(clazz));
    }

    public Slf4jLogImpl(final String category) {
        this(LOCALE, LoggerFactory.getLogger(category));
    }

    public Slf4jLogImpl(final Locale locale, final Logger logger) {
        this.logger = logger;
        this.locale = locale;
    }

    @Override
    public boolean isFatalEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void fatal(final String msg) {
        logger.error(msg);
    }

    @Override
    public void fatal(final String msg, final Object... args) {
        final String formatted = String.format(locale, msg, args);
        logger.error(formatted);
    }

    @Override
    public void fatal(final String msg, final Throwable throwable) {
        logger.error(msg, throwable);
    }

    @Override
    public void error(final String msg) {
        logger.error(msg);
    }

    @Override
    public void error(final String format, final Object... args) {
        final String formatted = String.format(locale, format, args);
        logger.error(formatted);
    }

    @Override
    public void error(final String msg, final Throwable throwable) {
        logger.error(msg, throwable);
    }

    @Override
    public void warn(final String msg) {
        logger.warn(msg);
    }

    @Override
    public void warn(final String msg, final Object... args) {
        final String formatted = String.format(locale, msg, args);
        logger.warn(formatted);
    }

    @Override
    public void warn(final String msg, final Throwable throwable) {
        logger.warn(msg, throwable);
    }

    @Override
    public void info(final String msg) {
        logger.info(msg);
    }

    @Override
    public void info(final String msg, final Object... args) {
        final String formatted = String.format(locale, msg, args);
        logger.info(formatted);
    }

    @Override
    public void debug(final String msg) {
        logger.debug(msg);
    }

    @Override
    public void debug(final String msg, final Object... args) {
        final String formatted = String.format(locale, msg, args);
        logger.debug(formatted);
    }

    @Override
    public void debug(final String msg, final Throwable throwable) {
        logger.debug(msg, throwable);
    }

    @Override
    public void trace(final Object msg) {
        logger.trace("{}", msg);
    }

    @Override
    public void trace(final String msg) {
        logger.trace(msg);
    }

    @Override
    public void trace(final String msg, final Object... args) {
        final String formatted = String.format(locale, msg, args);
        logger.trace(formatted);
    }

    @Override
    public void trace(final String msg, final Throwable throwable) {
        logger.trace(msg, throwable);
    }

    @Override
    public void setLevel(final String level) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLevel() {
        String result = "NONE";

        for (final Map.Entry<Function<Logger, Boolean>, String> entry : LEVELS.entrySet()) {
            if (entry.getKey().apply(logger)) {
                result = entry.getValue();
            } else {
                break;
            }
        }

        return result;
    }
}
