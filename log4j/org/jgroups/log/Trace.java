// $Id: Trace.java,v 1.3 2004/02/24 15:56:33 belaban Exp $

package org.jgroups.log;


import java.io.*;
import java.net.*;

import org.apache.log4j.*;


/**
 * Replacement for regular JGroups Trace, beased on log4j. log4j.properties should be somewhere on
 * the CLASSPATH
 * @author Bela Ban
 */
public class Trace {


    static {
        if(Logger.getRootLogger().getEffectiveLevel() == Level.OFF) {
            Trace.trace=false;
        }
        else {
            Trace.trace=true;
        }
    }


    // static int[] jgroups_levels={DEBUG,       TEST,        INFO,       WARN,       ERROR,       FATAL};
    static Level[] log4j_levels={Level.DEBUG, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL};


    static Level jgToLog4jLevel(int jg_level) {
        return log4j_levels[jg_level];
    }


    /** Users of Trace can check this flag to determine whether to trace. Example:
     <pre>
     if(Trace.trace)
     Trace.info("UDP.run()", "sending message");
     </pre>
     */
    public static boolean trace=true;


    /** Allows for conditional compilation, e.g. if(Trace.debug) Trace.info(...) would be removed from the code
     (if recompiled) when this flag is set to false. Therefore, code that should be removed from the final
     product should use if(Trace.debug) rather than if(Trace.trace).
     */
    public static final boolean debug=false;


    /**
     * Used to determine whether to copy messages (copy=true) in retransmission tables,
     * or whether to use references (copy=false). Once copy=false has worked for some time, this flag
     * will be removed entirely
     */
    public static final boolean copy=false;

    static String identifier=null;


    /** The trace level for all messages. */
    public static final int DEBUG=0;
    /** The trace level for all messages except DEBUG. */
    public static final int TEST=1;
    /** The trace level for all messages except DEBUG and TEST. */
    public static final int INFO=2;
    /** The trace level for all WARN, ERROR, and FATAL messages. */
    public static final int WARN=3;
    /** The trace level for all ERROR and FATAL messages. */
    public static final int ERROR=4;
    /** The trace level for only FATAL messages. */
    public static final int FATAL=5;


    /** Completely private constructor; should never be called. */
    private Trace() {
    }


    public static void init(String fname) throws IOException, SecurityException {
        ;
    }


    /**
     */
    public static void init() {
        ;
    }


    public static void flushStdouts() {
    }

    public static void setAutoFlushForAllStdouts(boolean auto) {
    }


    public static void flushStderrs() {
    }


    public static void setAutoFlushForAllStderrs(boolean auto) {
    }


    public static void setIdentifier(String id) {
        identifier=id;
    }

    public static void setTrace(boolean t) {
        trace=t;
    }


    /**
     * Changes the trace level for <code>module</code>. The output destination
     * is not changed. No other module (either higher or lower in the module
     * "inheritance chain") is affected. If this module has no output
     * associated with it (and thus any output would go to the current
     * default output), nothing happens. If the module name is not well-formed,
     * nothing happens.
     *
     * @param module a module name
     * @param level a trace level
     */
    public static void setOutput(String module, int level) {
    }

    /**
     * Sends a module's output to the specified file. Output is only sent
     * when a call to <code>print</code> or <code>println</code> specifies
     * a trace level &gt;= <code>level</code>.
     * <p>
     * If <code>fileName</code> is the name of a directory, a file name will
     * be generated using <code>module</code> and <code>level</code> (see
     * @link{#makeOutputFileName}).
     * <p>
     * If the file does not exist, it is created. If the file does exist, the
     * file is opened for appending.
     *
     * @param module a module name
     * @param level a trace level
     * @param fileName a file or directory name
     * @throws IOException when a <code>FileWriter</code> can not be created
     */
    public static void setOutput(String module, int level, String fileName) throws IOException {
        ;
    }

    /**
     * Sends a module's output to the specified <code>PrintStream</code>.
     * Output is only sent when a call to <code>print</code> or
     * <code>println</code> specifies a trace level &gt;= <code>level</code>.
     * <p>
     * The most common values of <code>outputStream</code> will be
     * <code>System.out</code> or <code>System.err</code>.
     * In fact, that is so likely that we keep pre-generated outputs around for
     * those two and return one when <code>outputStream</code> is one of those
     * two values.
     *
     * @param module a module name
     * @param level a trace level
     * @param outputStream a <code>PrintStream</code>
     */
    public static void setOutput(String module, int level, PrintStream outputStream) {
    }

    /**
     * Sends a module's output to the specified <code>PrintWriter</code>.
     * Output is only sent when a call to <code>print</code> or
     * <code>println</code> specifies a trace level &gt;= <code>level</code>.
     *
     * @param module a module name
     * @param level a trace level
     * @param writer a <code>PrintWriter</code>
     */
    public static void setOutput(String module, int level, PrintWriter writer) {
    }

    /**
     * Sends a module's output to the socket at <code>host</code> on
     * <code>port</code>. As with the <code>Socket</code> class, if
     * <code>port</code> is zero then the first available port will be used.
     * Output is only sent when a call to <code>print</code> or
     * <code>println</code> specifies a trace level &gt;= <code>level</code>.
     *
     * @param module a module name
     * @param level a trace level
     * @param host the name of a host machine
     * @param port a port number
     * @throws UnknownHostException if <code>host</code> can not be resolved
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static void setOutput(String module, int level, String host, int port)
            throws UnknownHostException, IOException {
        ;
    }

    /**
     * Sends a module's output to the socket at <code>addr</code> on
     * <code>port</code>. As with the <code>Socket</code> class, if
     * <code>port</code> is zero then the first available port will be used.
     * Output is only sent when a call to <code>print</code> or
     * <code>println</code> specifies a trace level &gt;= <code>level</code>.
     *
     * @param module a module name
     * @param level a trace level
     * @param addr an <code>InetAddress</code>
     * @param port a port number
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static void setOutput(String module, int level, InetAddress addr, int port)
            throws IOException {
    }

    /**
     * Closes the output for the specified module. When the output is closed,
     * it flushes its output first. It's OK to call this for modules that are
     * using <code>STDOUT</code>, <code>STDERR</code>, or the default output.
     *
     * @param module a module name
     */
    public static void closeOutput(String module) {
    }

    /**
     * Closes all outputs for all modules. When an output is closed, it
     * flushes its output first.
     */
    public static void closeAllOutputs() {
    }

    /**
     * Flushes all output for the specified module.
     *
     * @param module a module name
     */
    public static void flushOutput(String module) {
    }

    /**
     * Flushes all output for all modules.
     */
    public static void flushAllOutputs() {

    }

    /**
     * Sets the auto-flush flag for the specified module's output.
     *
     * @param module a module name
     * @param auto if <code>true</code> auto-flushing is turned on
     */
    public static void setAutoFlush(String module, boolean auto) {
    }

    /**
     * Sets the auto-flush flag for all outputs.
     * <p>
     * The auto-flush value for <code>STDOUT</code> and <code>STDERR</code>
     * are also set.
     *
     * @param auto if <code>true</code> auto-flushing is turned on
     */
    public static void setAutoFlushAll(boolean auto) {
    }

    /**
     * Sets the auto-flush flag for the current default output.
     *
     * @param auto if <code>true</code> auto-flushing is turned on
     */
    public static void setAutoFlushDefault(boolean auto) {
    }

    /**
     * Finds the appropriate output for the specified module and sends
     * <code>message</code> to it. When looking for which output to use,
     * the following set of rules are used. If no output is found (the
     * returned output is <code>null</code>), then nothing is printed.
     *
     * <ol>
     *
     * <li>If the module name contains no method name (there are no parenthesis),
     * look for the output associated with this module name. If found, return it.
     * Else, return the default output (initially <code>null</code>).</li>
     *
     * <li>Look for the output associated with this module and method name,
     * excluding arguments if any.</li>
     *
     * <li>Strip off the method name and look for the output associated with the
     * class name.</li>
     *
     * <li>Use the default output (initially <code>null</code>).
     *
     * </ol>
     *
     * @param module a module name
     * @param level a trace level
     * @param message the string to be output
     */
    public static void print(String module, int level, String message) {
        if(identifier == null)
            Logger.getLogger(module).log(jgToLog4jLevel(level), message);
        else
            Logger.getLogger(identifier + "." + module).log(jgToLog4jLevel(level), message);
    }

    /**
     Prints the identifier (e.g. process name) after the level, before the message.
     */
    public static void print(String module, int level, String identifier, String message) {
        if(identifier == null)
            Logger.getLogger(module).log(jgToLog4jLevel(level), "[" + identifier + " ] " + message);
        else
            Logger.getLogger(identifier + "." + module).log(jgToLog4jLevel(level), "[" + identifier + " ] " + message);
    }


    /**
     * Appends a line separator to <code>message</code> and calls
     * <code>print</code>. The system property "line.separator" is used;
     * this is not necessarily a single newline character ('\n').
     *
     * @param module a module name
     * @param level a trace level
     * @param message the string to be output
     * @see #print
     */
    public static void println(String module, int level, String message) {
        if(identifier == null)
            Logger.getLogger(module).log(jgToLog4jLevel(level), message);
        else
            Logger.getLogger(identifier + "." + module).log(jgToLog4jLevel(level), message);
    }

    /** Helper method. Will call Trace.println(module, Trace.DEBUG, module) */
    public static void debug(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).debug(message);
        else
            Logger.getLogger(identifier + "." + module).debug(message);
    }

    /** Helper method. Will call Trace.println(module, Trace.TEST, module) */
    public static void test(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).debug(message);
        else
            Logger.getLogger(identifier + "." + module).debug(message);
    }

    /** Helper method. Will call Trace.println(module, Trace.INFO, module) */
    public static void info(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).info(message);
        else
            Logger.getLogger(identifier + "." + module).info(message);
    }

    /** Helper method. Will call Trace.println(module, Trace.WARN, module) */
    public static void warn(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).warn(message);
        else
            Logger.getLogger(identifier + "." + module).warn(message);
    }

    /** Helper method. Will call Trace.println(module, Trace.ERROR, module) */
    public static void error(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).error(message);
        else
            Logger.getLogger(identifier + "." + module).error(message);
    }

    /** Helper method. Will call Trace.println(module, Trace.FATAL, module) */
    public static void fatal(String module, String message) {
        if(identifier == null)
            Logger.getLogger(module).fatal(message);
        else
            Logger.getLogger(identifier + "." + module).fatal(message);
    }


    /**
     * Set the timestamp format. The <code>format</code>
     * string is that used by {@link java.text.SimpleDateFormat}. If
     * <code>format</code> is <code>null</code>, then the ISO 8601 date
     * format ("CCYY-MM-DDThh:mm:ss,s") is used.
     */
    public static void setTimestampFormat(String format) {
    }


    /**
     * Sets the default output back to its original value, <code>STDOUT</code>.
     */
    public static synchronized void restoreDefaultOutput() {
    }

    /**
     * Closes the current default output. All output with no other destination
     * is ignored.
     */
    public static synchronized void closeDefaultOutput() {
    }

    /**
     * Sets the default output to the specified file and trace level.
     * If <code>fileName</code> is the name of a directory, the file name
     * <code>DEFAULT_OUTPUT_FILE_NAME</code> ("default.out") is used.
     * <p>
     * If the file does not exist, it is created. If the file does exist, the
     * file is opened for appending.
     *
     * @param fileName a file or directory name
     * @throws IOException when a <code>FileWriter</code> can not be created
     */
    public static synchronized void setDefaultOutput(int level, String fileName)
            throws IOException {
    }

    /**
     * Sets the default output to the specified file. If <code>fileName</code>
     * is the name of a directory, the file name
     * <code>DEFAULT_OUTPUT_FILE_NAME</code> ("default.out") is used.
     * <p>
     * If the file does not exist, it is created. If the file does exist, the
     * file is opened for appending.
     *
     * @param fileName a file or directory name
     * @throws IOException when a <code>FileWriter</code> can not be created
     */
    public static synchronized void setDefaultOutput(String fileName)
            throws IOException {
    }

    /**
     * Sets the default output to the specified <code>PrintStream</code> and
     * trace level.
     * <p>
     * The most common values of <code>outputStream</code> will be
     * <code>System.out</code> or <code>System.err</code>.
     * In fact, that is so likely that we keep pre-generated outputs around for
     * those two and return one when <code>outputStream</code> is one of those
     * two values.
     *
     * @param outputStream a <code>PrintStream</code>
     */
    public static synchronized void setDefaultOutput(int level, PrintStream outputStream) {
    }

    /**
     * Sets the default output to the specified <code>PrintStream</code>.
     * <p>
     * The most common values of <code>outputStream</code> will be
     * <code>System.out</code> or <code>System.err</code>.
     * In fact, that is so likely that we keep pre-generated outputs around for
     * those two and return one when <code>outputStream</code> is one of those
     * two values.
     *
     * @param outputStream a <code>PrintStream</code>
     */
    public static synchronized void setDefaultOutput(PrintStream outputStream) {
    }

    /**
     * Sets the default output to the specified <code>PrintWriter</code> and
     * trace level.
     *
     * @param writer a <code>PrintWriter</code>
     */
    public static synchronized void setDefaultOutput(int level, PrintWriter writer) {

    }

    /**
     * Sets the default output to the specified <code>PrintWriter</code>.
     *
     * @param writer a <code>PrintWriter</code>
     */
    public static synchronized void setDefaultOutput(PrintWriter writer) {

    }

    /**
     * Sets the default output to the socket at <code>host</code> on
     * <code>port</code> with the given trace level. As with the
     * <code>Socket</code> class, if <code>port</code> is zero then the first
     * available port will be used.
     *
     * @param host the name of a host machine
     * @param port a port number
     * @throws UnknownHostException if <code>host</code> can not be resolved
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static synchronized void setDefaultOutput(int level, String host, int port)
            throws UnknownHostException, IOException {
    }

    /**
     * Sets the default output to the socket at <code>host</code> on
     * <code>port</code>. As with the <code>Socket</code> class, if
     * <code>port</code> is zero then the first available port will be used.
     *
     * @param host the name of a host machine
     * @param port a port number
     * @throws UnknownHostException if <code>host</code> can not be resolved
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static synchronized void setDefaultOutput(String host, int port)
            throws UnknownHostException, IOException {
    }

    /**
     * Sets the default output to the socket at <code>addr</code> on
     * <code>port</code> at the specified trace level. As with the
     * <code>Socket</code> class, if <code>port</code> is zero then the first
     * available port will be used.
     *
     * @param addr an <code>InetAddress</code>
     * @param port a port number
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static synchronized void setDefaultOutput(int level, InetAddress addr,
                                                     int port)
            throws IOException {
    }

    /**
     * Sets the default output to the socket at <code>addr</code> on
     * <code>port</code>. As with the <code>Socket</code> class, if
     * <code>port</code> is zero then the first available port will be used.
     *
     * @param addr an <code>InetAddress</code>
     * @param port a port number
     * @throws IOException if an I/O error occurs when creating a socket
     * @see java.net.Socket
     */
    public static synchronized void setDefaultOutput(InetAddress addr, int port)
            throws IOException {
    }


    /**
     * Converts an exception stack trace into a java.lang.String
     * @param x an exception
     * @return a string containg the stack trace, null if the exception parameter was null
     */
    public static String getStackTrace(Throwable x) {
        if(x == null)
            return null;
        else {
            java.io.ByteArrayOutputStream bout=new java.io.ByteArrayOutputStream();
            java.io.PrintStream writer=new java.io.PrintStream(bout);
            x.printStackTrace(writer);
            String result=new String(bout.toByteArray());
            return result;
        }
    }


}
