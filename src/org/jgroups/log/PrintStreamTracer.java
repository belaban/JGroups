// $Id: PrintStreamTracer.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.io.PrintStream;

    /**
     * Provides output to a <code>PrintStream</code>. All methods defined
     * here have permissions that are either protected or package.
     */
public class PrintStreamTracer extends Tracer
{

    /* stream to print to */
    protected PrintStream out;

    /**
     * Creates a PrintStreamTracer for a particular module, using the provided stream
     * The constructor is package specific, ie, it can not be used outside this package
     * @param module - the name of the module that uses this NetworkTracer
     * @param level  - the log level as defined in Tracer.java
     * @param stream - a print stream that will be printed to
     * @see org.jvagroups.log.Tracer
     */
    PrintStreamTracer(String module, int level, PrintStream stream)
    {
        super(module, level);
        out = stream;
    }

    /**
     * Sends the already-formatted <code>message</code> to the output file,
     * stream, writer, or socket associated with this tracer.
     *
     * @param message a formatted string
     */
    protected void doPrint(String message)
    {
        out.print(message);
    }

   /**
     * Flushes any pending output. Called from <code>flush</code>, but only
     * if not alread closed.
     * This call flushes the print stream of the socket
     */
    protected void doFlush()
    {
        out.flush();
    }

   /**
     * Flushes any pending output (by calling <code>flush</code>) and closes
     * the output stream associated with this tracer.
     * Called from <code>close</code>, but only if not alread closed.
     * <p>
     * After closed has been called, all calls to <code>print</code>,
     * <code>flush</code>, and <code>close</code> are ignored.
     */
    protected void doClose()
    {
        out.close();
    }

}
