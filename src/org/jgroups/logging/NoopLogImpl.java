package org.jgroups.logging;

/**
 * Provides a no-op {@link Log} implementation.
 * @author Bela Ban
 * @since  5.6.0
 */
public class NoopLogImpl implements Log {
    @Override
    public boolean isFatalEnabled() {return false;}

    @Override
    public boolean isErrorEnabled() {return false;}

    @Override
    public boolean isWarnEnabled() {return false;}

    @Override
    public boolean isInfoEnabled() {return false;}

    @Override
    public boolean isDebugEnabled() {return false;}

    @Override
    public boolean isTraceEnabled() {return false;}

    @Override
    public void fatal(String msg) {    }

    @Override
    public void fatal(String format, Object... args) {}

    @Override
    public void fatal(String msg, Throwable throwable) {}

    @Override
    public void error(String msg) {}

    @Override
    public void error(String format, Object... args) {}

    @Override
    public void error(String msg, Throwable throwable) {}

    @Override
    public void warn(String msg) {}

    @Override
    public void warn(String format, Object... args) {}

    @Override
    public void warn(String msg, Throwable throwable) {}

    @Override
    public void info(String msg) {}

    @Override
    public void info(String format, Object... args) {}

    @Override
    public void debug(String msg) {}

    @Override
    public void debug(String format, Object... args) {}

    @Override
    public void debug(String msg, Throwable throwable) {}

    @Override
    public void trace(Object obj) {}

    @Override
    public void trace(String msg) {}

    @Override
    public void trace(String format, Object... args) {}

    @Override
    public void trace(String msg, Throwable throwable) {}

    @Override
    public void setLevel(String level) {}

    @Override
    public String getLevel() {return "";}
}
