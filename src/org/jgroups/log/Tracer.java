// $Id: Tracer.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.util.Date;

/**
 * Provides output services for a module. Formats messages by adding a
 * timestamp, the trace level, and the module name. Sends formatted
 * messages to the file, stream, writer, or socket associated with an
 * instance of a concrete subclass of <code>Tracer</code>. Checks trace
 * level and does not send messages if the specified level is less than
 * the trace level set by a call to <code>Trace.setOutput</code>.
 *
 * @author Jim Menard, <a href="mailto:jimm@io.com">jimm@io.com</a>
 * @see Trace
 */
public abstract class Tracer {

    /**
     * Set by <code>Trace.setTimestampFormat()</code> and used in the
     * <code>timestamp</code> method. When <code>null</code>, the default
     * format of ISO 8601 is used.
     * @see Trace#setTimestampFormat
     * @see Format#formatTimestamp
     */
    protected static String timestampFormat = null;

    /** The module name with which this tracer is associated. */
    protected String module;
    /** The current output trace level. */
    protected int level;
    /**
     * If <code>true</code>, every call to <code>print</code> will call
     * <code>flush</code>. Initially <code>false</code>.
     */
    protected boolean autoFlush;
    /**
     * If <code>true</code>, this tracer has been closed and no further output
     * will be accepted.
     */
    protected boolean closed;

    /**
     * Set the timestamp format. The <code>format</code>
     * string is that used by {@link java.text.SimpleDateFormat}. If
     * <code>format</code> is <code>null</code>, then the ISO 8601 date
     * format ("CCYY-MM-DDThh:mm:ss,s") is used.
     */
    public static void setTimestampFormat(String format) {
	timestampFormat = format;
    }

    /**
     * Constructor.
     *
     * @param module a module name
     * @param trace a trace level
     */
    Tracer(String module, int level) {
	this.module = module;
	this.level = level;
	closed = false;
    }


    /**
     * Returns the module name with which this tracer is associated.
     *
     * @return a module name
     */
    public String getModule() { return module; }

    /**
     * Returns the current trace level.
     *
     * @return the current trace level
     */
    public int getLevel() { return level; }
    /**
     * Sets the trace level. Messages sent to this tracer whose level are
     * &gt;= <code>level</code> are output. All others are ignored.
     *
     * @param level the trace level
     */
    public void setLevel(int level) { this.level = level; }

    /**
     * Returns <code>true</code> if this tracer performs auto-flushing after
     * every print.
     *
     * @return <code>true</code> if this tracer performs auto-flushing
     */
    public boolean getAutoFlush() { return autoFlush; }
    /**
     * Sets the auto-flush flag. If <code>true</code>, every call to
     * <code>print</code> will also call <code>flush</code>.
     *
     * @param autoFlush if <code>true</code>, auto-flushing is turned on
     */
    public void setAutoFlush(boolean autoFlush) {
	this.autoFlush = autoFlush;
	if (autoFlush)
	    flush();
    }

    /**
     * Sends a formatted string to the output file, stream, writer, or socket.
     * If <code>level</code> is less than the current trace level, the message
     * is ignored. If <code>autoFlush</code> is <code>true</code>, calls
     * <code>flush</code>.
     * <p>
     * Calls the abstract method <code>doPrint</code>, which is overridden by
     * concrete subclasses to perform the actual output.
     *
     * @param module a module name
     * @param level a trace level
     * @param message the string to be output
     */
    public void print(String module, int level, String message) {
	if (level >= this.level) {
	    doPrint(logString(module, level, message));
	    if (autoFlush)
		flush();
	}
    }


    public void print(String module, int level, String identifier, String message) {
	if (level >= this.level) {
	    doPrint(logString(module, level, identifier, message));
	    if (autoFlush)
		flush();
	}
    }

    /**
     * Flushes any pending output.
     */
    public void flush() {
	if (!closed)
	    doFlush();
    }

    /**
     * Flushes any pending output (by calling <code>flush</code>) and closes
     * the output file, stream, writer, or socket associated with this tracer.
     * <p>
     * After this method has been called, all calls to <code>print</code>,
     * <code>flush</code>, and <code>close</code> are ignored.
     */
    public void close() {
	if (!closed) {
	    flush();
	    doClose();
	    closed = true;
	}
    }

    /**
     * Creates a formatted string suitable for output. The string contains
     * a timestamp, a string version of the trace level, and the module name.
     *
     * @param module a module name
     * @param level a trace level
     * @param message the message to be included
     * @return a formatted string containing timestamp, level, module name,
     * and message
     */
    protected String logString(String module, int level, String message) {
	StringBuffer sb=new StringBuffer();
	sb.append("[").append(timestamp()).append("] [").append(Format.levelToString(level)).append("] ");
	sb.append(module).append(": ").append(message);
	return sb.toString();
    }

    protected String logString(String module, int level, String identifier, String message) {
	StringBuffer sb=new StringBuffer();
	sb.append("[").append(timestamp()).append("] [").append(Format.levelToString(level)).append("] ");
	sb.append("[").append(identifier).append("] ").append(module).append(": ").append(message);
	return sb.toString();
    }

    /**
     * Returns a timestamp string.
     *
     * @return an ISO 8601 time string
     */
    protected String timestamp() {
	// return Format.formatTimestamp(new Date(), null);
	return Format.formatTimestamp(new Date(), timestampFormat);  // (changed by bela)
    }

    /**
     * Sends the already-formatted <code>message</code> to the output file,
     * stream, writer, or socket associated with this tracer.
     *
     * @param message a formatted string
     */
    protected abstract void doPrint(String message);

    /**
     * Flushes any pending output. Called from <code>flush</code>, but only
     * if not alread closed.
     */
    protected abstract void doFlush();

    /**
     * Flushes any pending output (by calling <code>flush</code>) and closes
     * the output file, stream, writer, or socket associated with this tracer.
     * Called from <code>close</code>, but only if not alread closed.
     * <p>
     * After closed has been called, all calls to <code>print</code>,
     * <code>flush</code>, and <code>close</code> are ignored.
     */
    protected abstract void doClose();

}
