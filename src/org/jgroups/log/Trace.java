// $Id: Trace.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Provides timestamped logging to files, streams, writers, and sockets
 * at differing levels of verbosity. Log outputs are associated with a
 * module name (see this package's description) and a trace level.
 * <p>
 * Since tracing statements create a lot of strings, even when tracing is disabled, a trace statement should
 * always be conditional, e.g. <code>if(Trace.trace) Trace.info("UDP.run()", "bla")</code>. Thus, if tracing
 * is disabled, the strings won't be created. For trace statements that should always be printed,
 * we don't need to make the trace statement conditional. Note that, if trace statements should be removed in
 * the production code, we can use the Trace.debug flag, e.g. <code>if(Trace.debug) Trace(...)</code>. If
 * Trace.debug is set to false when the code is compiled, the entire statement will not be added to the
 * generated classfile.
 * <p>
 * A simple example of using <code>Trace</code>:
 * <pre>
 * <font color="blue">// Send all WARN and higher messages to /tmp/logfile for MyClass</font>
 * Trace.setOutput("MyClass", Trace.WARN, "/tmp/logfile");
 *
 * <font color="blue">// Send all messages to /tmp/AnotherClass_DEBUG for AnotherClass</font>
 * Trace.setOutput("AnotherClass", Trace.DEBUG, "/tmp");
 *
 * <font color="blue">// Printed to /tmp/logfile because FATAL &gt; WARN</font>
 * Trace.println("MyClass", Trace.FATAL, "help!");
 *
 * <font color="blue">// Not printed because DEBUG &lt; WARN (the current level for MyClass)</font>
 * Trace.println("MyClass", Trace.DEBUG, "blah, blah");
 *
 * <font color="blue">// Printed to /tmp/AnotherClass_DEBUG because of the</font>
 * <font color="blue">// "module inheritance" rules (see package overview).</font>
 * Trace.println("AnotherClass.myMethod()", Trace.DEBUG, "bletch");
 *
 * <font color="blue">// Close the outputs separately. Could also call Trace.closeAllOutputs()</font>
 * Trace.closeOutput("MyClass");
 * Trace.closeOutput("AnotherClass");
 * </pre>
 *
 * <p>
 * For a more complete discussion of module names and the rules used to
 * find the output associated with a module, see the Description section
 * of the overview for this package.
 *
 * @author Jim Menard, <a href="mailto:jimm@io.com">jimm@io.com</a>
 */
public class Trace {

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


    /** The trace level for all messages. */
    public static final int DEBUG = 0;
    /** The trace level for all messages except DEBUG. */
    public static final int TEST = 1;
    /** The trace level for all messages except DEBUG and TEST. */
    public static final int INFO = 2;
    /** The trace level for all WARN, ERROR, and FATAL messages. */
    public static final int WARN = 3;
    /** The trace level for all ERROR and FATAL messages. */
    public static final int ERROR = 4;
    /** The trace level for only FATAL messages. */
    public static final int FATAL = 5;

    /**
     * The file name used when setting a new default output and when the
     * file name specified is a directory.
     */
    protected static final String DEFAULT_OUTPUT_FILE_NAME = "default.out";
    protected static final String TRACE_STMT="trace";
    protected static final String DEFAULT_OUTPUT_STMT="default_output";
    protected static final String TIMESTAMP_FORMAT_STMT="timestamp_format";
    protected static final String STDOUT_STMT="STDOUT";
    protected static final String STDERR_STMT="STDERR";
    protected static final String DEFAULT_OUTPUT="default_output";


    /** Completely private constructor; should never be called. */
    private Trace() {}

    /** Maps module names to <code>Tracer</code>s. */
    protected static final HashMap TRACERS = new HashMap();


    /**
     * We initially create Tracers for each STDOUT level
     */
    protected static final Tracer[] STDOUTS={
	new SystemOutTracer(DEBUG),
	new SystemOutTracer(TEST),
	new SystemOutTracer(INFO),
	new SystemOutTracer(WARN),
	new SystemOutTracer(ERROR),
	new SystemOutTracer(FATAL)
    };


    /**
     * We initially create Tracers for each STDERR level
     */
    protected static final Tracer[] STDERRS={
	new SystemErrTracer(DEBUG),
	new SystemErrTracer(TEST),
	new SystemErrTracer(INFO),
	new SystemErrTracer(WARN),
	new SystemErrTracer(ERROR),
	new SystemErrTracer(FATAL)
    };


    /**
     * The default tracer to use when a message is not destined for any
     * particular output. May be <code>null</code>; defaults to
     * <code>null</code>.
     */
    protected static Tracer defaultTracer = null;  // changed by Bela Ban

    /**
     * The newline string used in <code>println</code>. This is the system
     * property <code>line.separator</code>, and is not necessarily the
     * newline character ('\n').
     *
     * @see #println
     */
    protected static final String NEWLINE = System.getProperty("line.separator");


    /**
     * The character used to denote directories (e.g. "/" in UNIX and '\' in Windows
     */
    protected static final String FILE_SEPARATOR= System.getProperty("file.separator");


    /**
       Used to e.g. identify the process which uses Trace. There might be multiple processes using the same
       log files.
    */
    protected static String identifier=null;


    /**
     * Flushes and closes all open outputs. This code will get run once at
     * VM shutdown time.<p>
     * Can only be used in JDK >= 1.3 !
     *
     * @see Runtime#addShutdownHook
     */
    static {
	Runtime.getRuntime().addShutdownHook(new Thread() {
	    public void run() {
		Trace.closeAllOutputs();
	    }
	});
    }




    /**
       Reads a property file and sets outputs, default outputs, timestamp format etc. The file has to be in the format
       parsable by java.util.Properties. Each output has to have the form <code>traceX</code> where <x> must be unique for
       each statement. The following statements are recognized:
       <ol>
       <li><code>traceX=modulename level output</code>. <code>Modulename</code> is the name of the module,
       <code>level</code> must be one of <code>DEBUG</code>, <code>TEST</code>, <code>INFO</code>,
       <code>WARN</code>, <code>ERROR</code> or <code>FATAL</code> and output is either a filename/directory
       or <code>STDERR</code>/<code>STDOUT</code>
       <li><code>default_output=level output</code>.
       <li><code>timestamp_format=format</code>. Sets the format for timestamps, see
       {@link #setTimestampFormat}.
       </ol>
       @exception IOException Throw when <code>fname</code> does not exist
       @exception SecurityException Thrown when file exists, but is not accessible
    */
    private static void init(InputStream in) throws IOException, SecurityException {
        Properties p;
        Map.Entry entry;

        p=new Properties();
        p.load(in);
        for(Iterator it=p.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            parse((String)entry.getKey(), (String)entry.getValue());
        }
        setAutoFlushAll(true);  // set if properties are read from file
        setAutoFlushDefault(true);
    }

    /**
     *
     * @param fname Fully qualified name of property file. If it is a directory, "/jgroups.properties"
       will be appended to the directory name.
     * @throws IOException
     * @throws SecurityException
     */
    public static void init(String fname) throws IOException, SecurityException {
        FileInputStream in;
        File            f;

        f=new File(fname);
        if(f.isDirectory())
            fname=fname + FILE_SEPARATOR + "jgroups.properties";
        in=new FileInputStream(fname);
        init( in );
    }


    /**
     * Same as init(String fname), but looks in user's home directory then in CLASSPATH for jgroups.properties.
     * Won't complain if not found.
     */
    public static void init() {
	String home_dir=System.getProperty("user.home");
	if(home_dir == null) {
	    System.err.println("Trace.init(): user's home directory (\"user.home\") was not set");
	}
	else {
	    home_dir=home_dir + FILE_SEPARATOR + "jgroups.properties";
	    try {
		init(home_dir);
		return;
	    }
	    catch(Exception ex) {
		System.err.println("Trace.init() " + ex);
	    }
	}

	try {	//first try to load from classpath
	    java.io.InputStream in = Trace.class.getResourceAsStream("/jgroups.properties");
	    if(in != null) {
		init(in);
		// System.out.println("Initialized trace properties from class path!");
		return;
	    }
	}
	catch(Exception x) {
	    x.printStackTrace();
	    System.err.println("Trace.init() " + x);
	}
    }



    public static Tracer getStdoutTracer(int level) {
	return STDOUTS[level];
    }

    public static void flushStdouts() {
	for(int i =0; i < STDOUTS.length; i++)
	    STDOUTS[i].flush();
    }

    public static void setAutoFlushForAllStdouts(boolean auto) {
	for(int i =0; i < STDOUTS.length; i++)
	    STDOUTS[i].setAutoFlush(auto);
    }


    public static Tracer getStderrTracer(int level) {
	return STDERRS[level];
    }


    public static void flushStderrs() {
	for(int i =0; i < STDERRS.length; i++)
	    STDERRS[i].flush();
    }

    
    public static void setAutoFlushForAllStderrs(boolean auto) {
	for(int i =0; i < STDERRS.length; i++)
	    STDERRS[i].setAutoFlush(auto);
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
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    tracer = lookup(module);
	}
	if (tracer != null && tracer != getDefaultTracer())
	    tracer.setLevel(level);
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
    public static void setOutput(String module, int level, String fileName)
	throws IOException
    {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    closeOutput(module);

	    if (fileName == null)
		return;

	    File f = new File(fileName);
	    if (f.isDirectory()) {
		fileName = makeOutputFileName(module, level);
		f = new File(f, fileName);
	    }

	    FileWriter out = new FileWriter(f.getAbsolutePath(), true);
	    tracer = new WriterTracer(module, level, out);

	    addTracer(module, tracer);
	}
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
    public static void setOutput(String module, int level,
				 PrintStream outputStream) {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    closeOutput(module);

	    if (outputStream == null)
		tracer = null;
	    else if (outputStream.equals(System.err))
		tracer=getStderrTracer(level);
	    else if (outputStream.equals(System.out)) {
		tracer=getStdoutTracer(level);
	    }
	    else
		tracer = new WriterTracer(module, level, new PrintWriter(outputStream));
	    addTracer(module, tracer);
	}
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
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    closeOutput(module);
	    tracer = writer == null ? null :
		new WriterTracer(module, level, writer);
	    addTracer(module, tracer);
	}
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
	throws UnknownHostException, IOException
    {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    closeOutput(module);
	    tracer = host == null ? null :
		new NetworkTracer(module, level, new Socket(host, port));
	    addTracer(module, tracer);
	}
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
	throws IOException
    {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = null;
	synchronized(TRACERS) {
	    closeOutput(module);
	    tracer = addr == null ? null :
		new NetworkTracer(module, level, new Socket(addr, port));
	    addTracer(module, tracer);
	}
    }

    /**
     * Closes the output for the specified module. When the output is closed,
     * it flushes its output first. It's OK to call this for modules that are
     * using <code>STDOUT</code>, <code>STDERR</code>, or the default output.
     *
     * @param module a module name
     */
    public static void closeOutput(String module) {
	if (bogusModuleName(module))
	    return;

	synchronized(TRACERS) {
	    Tracer tracer = lookup(module);
	    if (tracer != null && tracer != getDefaultTracer())
		// It's OK to call on STDOUT and STDERR; nothing happens.
		tracer.close();
	    removeTracer(module);
	}
    }

    /**
     * Closes all outputs for all modules. When an output is closed, it
     * flushes its output first.
     */
    public static void closeAllOutputs() {
	synchronized(TRACERS) {
	    for (Iterator iter = TRACERS.values().iterator(); iter.hasNext(); ) {
		Tracer tracer = (Tracer)iter.next();
		tracer.close();
	    }
	    TRACERS.clear();
	}
    }

    /**
     * Flushes all output for the specified module.
     *
     * @param module a module name
     */
    public static void flushOutput(String module) {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = lookup(module);
	if (tracer != null)
	    tracer.flush();
    }

    /**
     * Flushes all output for all modules.
     */
    public static void flushAllOutputs() {
	for (Iterator iter = TRACERS.values().iterator(); iter.hasNext(); ) {
	    Tracer tracer = (Tracer)iter.next();
	    tracer.flush();
	}
	flushStdouts();
	flushStderrs();
    }

    /**
     * Sets the auto-flush flag for the specified module's output.
     *
     * @param module a module name
     * @param auto if <code>true</code> auto-flushing is turned on
     */
    public static void setAutoFlush(String module, boolean auto) {
	if (bogusModuleName(module))
	    return;

	Tracer tracer = lookup(module);
	if (tracer != null)
	    tracer.setAutoFlush(auto);
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
	for (Iterator iter = TRACERS.values().iterator(); iter.hasNext(); ) {
	    Tracer tracer = (Tracer)iter.next();
	    tracer.setAutoFlush(auto);
	}
	setAutoFlushForAllStdouts(auto);
	setAutoFlushForAllStderrs(auto);
    }

    /**
     * Sets the auto-flush flag for the current default output.
     *
     * @param auto if <code>true</code> auto-flushing is turned on
     */
    public static void setAutoFlushDefault(boolean auto) {
	Tracer tracer = getDefaultTracer();
	if (tracer != null)
	    tracer.setAutoFlush(auto);
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
	if(defaultTracer == null && TRACERS.size() == 0) return;

	Tracer tracer = findTracerFor(module);
	if (tracer != null)
	    tracer.print(module, level, message);
    }

    /**
       Prints the identifier (e.g. process name) after the level, before the message.
    */
    public static void print(String module, int level, String identifier, String message) {
	if(defaultTracer == null && TRACERS.size() == 0) return;

	Tracer tracer = findTracerFor(module);
	if (tracer != null)
	    tracer.print(module, level, identifier, message);
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
	if(identifier != null)
	    print(module, level, identifier, message + NEWLINE);
	else
	    print(module, level, message + NEWLINE);
    }

    /** Helper method. Will call Trace.println(module, Trace.DEBUG, module) */
    public static void debug(String module, String message) {
	println(module, Trace.DEBUG, message);
    }

    /** Helper method. Will call Trace.println(module, Trace.TEST, module) */
    public static void test(String module, String message) {
	println(module, Trace.TEST, message);
    }

    /** Helper method. Will call Trace.println(module, Trace.INFO, module) */
    public static void info(String module, String message) {
	println(module, Trace.INFO, message);
    }

    /** Helper method. Will call Trace.println(module, Trace.WARN, module) */
    public static void warn(String module, String message) {
	println(module, Trace.WARN, message);
    }

    /** Helper method. Will call Trace.println(module, Trace.ERROR, module) */
    public static void error(String module, String message) {
	println(module, Trace.ERROR, message);
    }

    /** Helper method. Will call Trace.println(module, Trace.FATAL, module) */
    public static void fatal(String module, String message) {
	println(module, Trace.FATAL, message);
    }





    /**
     * Set the timestamp format. The <code>format</code>
     * string is that used by {@link java.text.SimpleDateFormat}. If
     * <code>format</code> is <code>null</code>, then the ISO 8601 date
     * format ("CCYY-MM-DDThh:mm:ss,s") is used.
     */
    public static void setTimestampFormat(String format) {
	Tracer.setTimestampFormat(format);
    }

    /**
     * Returns the default <code>Tracer</code> to be used when no other
     * appropriate one is found. May be <code>null</code>
     *
     * @return the default <code>Tracer</code> (<code>defaultTracer</code>);
     * may be <code>null</code>
     * @see Tracer
     */
    protected static Tracer getDefaultTracer() {
	return defaultTracer;
    }

    /**
     * Sets the default output back to its original value, <code>STDOUT</code>.
     */
    public static synchronized void restoreDefaultOutput() {
	if (defaultTracer != null)
	    defaultTracer.close();	// Harmless when tracer is STDOUT or STDERR
	//defaultTracer = getStdoutTracer(DEBUG);
	defaultTracer = null;
    }

    /**
     * Closes the current default output. All output with no other destination
     * is ignored.
     */
    public static synchronized void closeDefaultOutput() {
	if (defaultTracer != null)
	    defaultTracer.close();	// Harmless when tracer is STDOUT or STDERR
	defaultTracer = null;
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
	throws IOException
    {
	closeDefaultOutput();

	if (fileName != null) {
	    File f = new File(fileName);
	    if (f.isDirectory())
		f = new File(f, DEFAULT_OUTPUT_FILE_NAME);

	    FileWriter out = new FileWriter(f.getAbsolutePath(), true);
	    defaultTracer = new WriterTracer("", level, out);
	}
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
	throws IOException
    {
	setDefaultOutput(DEBUG, fileName);
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
	closeDefaultOutput();

	if (outputStream == null)
	    defaultTracer = null;
	else if (outputStream.equals(System.err))
	    defaultTracer = getStderrTracer(level);
	else if (outputStream.equals(System.out))
	    defaultTracer = getStdoutTracer(level);
	else
	    defaultTracer = new WriterTracer("", level, new PrintWriter(outputStream));
	defaultTracer.setLevel(level);
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
	setDefaultOutput(DEBUG, outputStream);
    }

    /**
     * Sets the default output to the specified <code>PrintWriter</code> and
     * trace level.
     *
     * @param writer a <code>PrintWriter</code>
     */
    public static synchronized void setDefaultOutput(int level, PrintWriter writer) {
	closeDefaultOutput();
	defaultTracer = (writer == null) ? null : new WriterTracer("", level, writer);
	if(defaultTracer != null)
	    defaultTracer.setLevel(level);
    }

    /**
     * Sets the default output to the specified <code>PrintWriter</code>.
     *
     * @param writer a <code>PrintWriter</code>
     */
    public static synchronized void setDefaultOutput(PrintWriter writer) {
	setDefaultOutput(DEBUG, writer);
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
	throws UnknownHostException, IOException
    {
	closeDefaultOutput();
	defaultTracer = (host == null) ? null :
	    new NetworkTracer("", level, new Socket(host, port));
	if(defaultTracer != null)
	    defaultTracer.setLevel(level);
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
	throws UnknownHostException, IOException
    {
	setDefaultOutput(DEBUG, host, port);
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
	throws IOException
    {
	closeDefaultOutput();
	defaultTracer = (addr == null) ? null :
	    new NetworkTracer("", level, new Socket(addr, port));
	if(defaultTracer != null)
	    defaultTracer.setLevel(level);
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
	throws IOException
    {
	setDefaultOutput(DEBUG, addr, port);
    }

    /**
     * Associates the specified module name with a <code>Tracer</code>.
     *
     * @param module a module name
     * @param tracer a <code>Tracer</code>
     * @see Tracer
     */
    public static void addTracer(String module, Tracer tracer) {
	synchronized(TRACERS) {
	    TRACERS.put(moduleLookupName(module), tracer);
	}
    }

    /**
     * Removes the <code>Tracer</code> for the specified module.
     * Does not close or otherwise modify the <code>Tracer</code>.
     *
     * @param module a module name
     * @see Tracer
     */
    protected static void removeTracer(String module) {
	synchronized(TRACERS) {
	    TRACERS.remove(moduleLookupName(module));
	}
    }

    /**
     * Returns the <code>Tracer</code> that matches the specified module
     * name. No rule matching is done; if the module name as returned
     * by <code>moduleLookupName</code> is not found, <code>null</code>
     * is returned. This method is called from <code>setOutput</code> but
     * not from <code>print</code>.
     *
     * @param module a module name
     * @return the <code>Tracer</code> associated with this module, or
     * <code>null</code> if not found
     * @see Tracer
     * @see #findTracerFor
     */
    protected static Tracer lookup(String module) {
	synchronized(TRACERS) {
	    return (Tracer)TRACERS.get(moduleLookupName(module));
	}
    }

    /**
     * Returns the string to be used to store and look up the specified module.
     * The string returned is a copy of <code>module</code> with everything
     * after first parenthesis chopped off. The first parenthesis is kept.
     * This method is called from <code>setOutput</code> but not from
     * <code>print</code>.
     * <p>
     * Keeping the parenthesis helps us distinguish between modules that have
     * method names in them and fully qualified class names. As an admittedly
     * artificial example, the module named "Foo.Method()" would be
     * distinguishable from a class whose name is Method and full path is
     * "Foo.Method".
     *
     * @param module a module name
     * @return a copy of the module name suitable for <code>Tracer</code> lookup
     * and storage
     * @see Tracer
     */
    protected static String moduleLookupName(String module) {
	int parenPos = module.indexOf('(');
	return (parenPos == -1) ? module : module.substring(0, parenPos + 1);
    }

    /**
     * Returns the <code>Tracer</code> to be used for output for the specified
     * module name. Performs a series of searches described in the comment for
     * <code>print</code>. May return <code>null</code>, if no match is found
     * and <code>getDefaultTracer</code> returns <code>null</code>. This method
     * is called from <code>print</code>, and is not called from
     * <code>setOutput</code>.
     *
     * @param module a module name
     * @return a <code>Tracer</code> or <code>null</code> if there are no
     * matches and the default tracer is <code>null</code>
     * @see Tracer
     * @see #lookup
     * @see #getDefaultTracer */
    protected static Tracer findTracerFor(String module) {
	if (bogusModuleName(module))
	    return getDefaultTracer();

	Tracer tracer = null;
	int parenPos = module.indexOf('(');

	if (parenPos == -1) {
	    // We have an module name with no method. Either match exactly
	    // or give up and use the default output.
	    tracer = (Tracer)TRACERS.get(module);
	    return tracer == null ? getDefaultTracer() : tracer;
	}

	// Try to match everything up to and including the paren.
	module = module.substring(0, parenPos);
	tracer = (Tracer)TRACERS.get(module);
	if (tracer == null) {
	    // Look for module without method
	    int dotPos = module.lastIndexOf('.');
	    while (dotPos != -1 && tracer == null) {
		module = module.substring(0, dotPos);
		tracer = (Tracer)TRACERS.get(module);
		dotPos = module.lastIndexOf('.');
	    }
	}

	if (tracer == null)		// No match; use the default tracer
	    tracer = getDefaultTracer();
	return tracer;
    }

    /**
     * Returns a file name created from a module name and trace level. The file
     * name is of the form <var>"module_LEVEL"</var>, where "module" is of the
     * form "module" or "module.method" without any parenthesis, and "LEVEL" is
     * a string like "DEBUG" or "FATAL". The returned value is not a full path,
     * just a string suitable for use as a file name.
     *
     * @param module a module name
     * @param level a trace level (DEBUG, FATAL, etc.)
     * @return a string created from the module name and trace level, suitable
     * for use as a file name
     */
    protected static String makeOutputFileName(String module, int level) {
	module = moduleLookupName(module);
	int pos = module.indexOf('('); // Strip off trailing paren, if any
	if (pos != -1)
	    module = module.substring(0, pos);
	return module + "_" +  Format.levelToString(level);
    }

    /**
     * Returns <code>true</code> if the specified module name is illegal.
     * Currently only checks to see that if the name contains '(' that there
     * is a '.' before it.
     *
     * @param module a module name
     * @return <code>true</code> if the specified module name is illegal,
     * else returns <code>false</code>
     */
    protected static boolean bogusModuleName(String module) {
	int parenPos = module.indexOf('(');
	if (parenPos != -1) {
	    int dotPos = module.indexOf('.');
	    if (dotPos == -1)
		return true;	// No dot before paren
	}
	return false;
    }



    protected static void parse(String key, String val) {
	if(key == null || val == null) {
	    System.err.println("Trace.Parse(): key or val is null");
	    return;
	}
	key=key.toLowerCase();
	if(key.equals(TRACE_STMT)) {
	    trace=new Boolean(val).booleanValue();
	}
	else if(key.startsWith(TRACE_STMT)) {
	    parseTrace(val);
	}
	else if(key.startsWith(DEFAULT_OUTPUT_STMT)) {
	    parseDefaultOutput(val);
	}
	else if(key.startsWith(TIMESTAMP_FORMAT_STMT)) {
	    parseTimestampFormat(val);
	}
	else {
	    System.err.println("Trace.Parse(): statement \"" + key + "\" not valid");
	}
    }


    /** Format is <module> <level> <fileame or STERR/STDOUT>. Argument is guaranteed not to be null */
    protected static void parseTrace(String val) {
	StringTokenizer tok=new StringTokenizer(val);
	String          module, level, output, tmp;
	int             lvl=-1;
	PrintStream     outstream=null;

	module=tok.nextToken();
	level=tok.nextToken();
	output=tok.nextToken();

	// System.out.println("module=" + module + ", level=" + level + ", output=" + output);
	lvl=string2Level(level);
	if(lvl == -1)
	    return;
	tmp=output.toLowerCase();
	if(tmp.equals("stdout")) outstream=System.out;
	else if(tmp.equals("stderr")) outstream=System.err;

	if(outstream != null)
	    Trace.setOutput(module, lvl, outstream);
	else {
	    try {
		Trace.setOutput(module, lvl, output);
	    }
	    catch(Exception ex) {
		System.err.println("Trace.ParseTrace() " + ex);
	    }
	}
    }

    protected static void parseDefaultOutput(String val) {
	StringTokenizer tok=new StringTokenizer(val);
	String          level, output, tmp;
	PrintStream     outstream=null;
	int             lvl=-1;

	level=tok.nextToken();
	output=tok.nextToken();

	lvl=string2Level(level);
	if(lvl == -1)
	    return;
	tmp=output.toLowerCase();
	if(tmp.equals("stdout")) outstream=System.out;
	else if(tmp.equals("stderr")) outstream=System.err;

	if(outstream != null)
	    Trace.setDefaultOutput(lvl, outstream);
	else {
	    try {
		Trace.setDefaultOutput(lvl, output);
	    }
	    catch(Exception ex) {
		System.err.println("Trace.ParseDefaultOutput() " + ex);
	    }
	}
    }

    protected static void parseTimestampFormat(String val) {
	Trace.setTimestampFormat(val);
    }

    protected static int string2Level(String l) {
	String tmp;

	if(l == null) return -1;
	tmp=l.toLowerCase();
	if(tmp.equals("debug")) return DEBUG;
	if(tmp.equals("test"))  return TEST;
	if(tmp.equals("info"))  return INFO;
	if(tmp.equals("warn"))  return WARN;
	if(tmp.equals("error")) return ERROR;
	if(tmp.equals("fatal")) return FATAL;
	System.err.println("Trace.String2Level(): level \"" + l + "\" is not known");
	return -1;
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
